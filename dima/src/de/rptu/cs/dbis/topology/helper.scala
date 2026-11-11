package de.rptu.cs.dbis.topology.helper

import de.rptu.cs.dbis.utils.resultsWriter.ResultsWriter
import de.rptu.cs.dbis.collection.redblacktree.RedBlackTree
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.component.indexer.Indexer
import better.files.*
import com.zaxxer.sparsebits.SparseBitSet
import org.roaringbitmap.buffer.MutableRoaringBitmap
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.domain.tuples.*
import cats.kernel.Order
import cats.Show
import de.rptu.cs.dbis.component.partition.{Partition, LeafOpsConfig}
import de.rptu.cs.dbis.collection.scanmap.SkipList
import de.rptu.cs.dbis.domain.TupleId
import scala.reflect.ClassTag
import de.rptu.cs.dbis.component.joiner.Joiner
import de.rptu.cs.dbis.component.storage.Storage
import org.apache.storm.topology.TopologyBuilder
import de.rptu.cs.dbis.domain.tuples.shuffler.SpoutTupleStream
import org.apache.storm.topology.IRichSpout
import de.rptu.cs.dbis.component.trigger.TriggerSpout
import de.rptu.cs.dbis.component.dispatcher.Dispatcher

import de.rptu.cs.dbis.component.metric.{MetricSpout, MetricWriter}
import de.rptu.cs.dbis.domain.tuples.metric.MetricKind
import org.apache.storm.topology.BoltDeclarer
import de.rptu.cs.dbis.domain.tuples.metric.MetricTriggerKind
import org.apache.storm.topology.base.BaseRichBolt
import de.rptu.cs.dbis.domain.timeTracker.{BoltWithTimeTracker, DurationPayload}
import de.rptu.cs.dbis.domain.tuples.attribute.Attribute
import scala.util.CommandLineParser
import de.rptu.cs.dbis.collection.scanmap.SkipListWithoutCache

enum PartitionStructure {
  case Skiplist, SkiplistWithoutCache, RedBlackTree
}

object PartitionStructure {
  given CommandLineParser.FromString[PartitionStructure] with {
    def fromString(s: String): PartitionStructure = {
      s match {
        case "Skiplist"             => PartitionStructure.Skiplist
        case "SkiplistWithoutCache" => PartitionStructure.SkiplistWithoutCache
        case "RedBlackTree"         => PartitionStructure.RedBlackTree
      }

    }
  }
}

enum SetStructure {
  case SparseBitSet, RoaringBitmap
}

object SetStructure {
  given CommandLineParser.FromString[SetStructure] with {
    def fromString(s: String): SetStructure = {
      s match {
        case "SparseBitSet"  => SetStructure.SparseBitSet
        case "RoaringBitmap" => SetStructure.RoaringBitmap
      }
    }
  }
}

case class AttrInfo(attr: Attribute, scanOp: ScanOperator) {
  val indexerId   = Indexer.componentId(attr.name)
  val partitionId = Partition.componentId(attr.name)
  val partitionMetricKind: MetricKind = metric.MetricKind(
    Partition.componentId(attr.name),
    s"partition-metrics-${attr.name}",
    s"partition-${attr.name}",
  )
}

case class MetricConfig(
  intervalMs: Int,
  metricKinds: Seq[MetricKind],
  timeTrackKinds: Seq[MetricKind],
) {
  val trackTime          = !timeTrackKinds.isEmpty
  val allMetricKinds     = metricKinds ++ timeTrackKinds
  val spout: MetricSpout = new MetricSpout(intervalMs)
  val kindsMap           = MetricKind.buildMap(allMetricKinds*)
  val components         = allMetricKinds.map(_.componentId).toSet
}

object MetricConfig {
  def apply(
    intervalMs: Int,
    metricKinds: Seq[MetricKind],
    attrsOp: Option[Seq[AttrInfo]],
  ): MetricConfig = {
    val timeTrackKinds = attrsOp match {
      case Some(attrs) => {
        (attrs.flatMap(ai => Seq(ai.indexerId, ai.partitionId)) ++
          Seq(Joiner.componentId, Storage.componentId))
          .map(DurationPayload.metricKind(_))
      }
      case None => Seq()
    }
    new MetricConfig(intervalMs, metricKinds, timeTrackKinds)
  }
}

extension (oc: Option[MetricConfig]) {
  def containsComponentId(id: String): Boolean = {
    oc.map(_.metricKinds)
      .toList
      .flatten
      .find(m => m.componentId == id)
      .isDefined
  }

  def makeBolt(componentId: String, b: BaseRichBolt): BaseRichBolt = {
    oc.map(c => {
        val otherMetricsId = c.metricKinds.map(_.componentId).toSet
        if (c.trackTime) {
          BoltWithTimeTracker(
            componentId,
            b,
            noOtherMetrics = !otherMetricsId.contains(componentId),
          )
        } else { b }
      })
      .getOrElse(b)

  }

  def activateBoltOnDemand(componentId: String)(b: BoltDeclarer): Unit = {
    oc.foreach(c => {
      if (c.components.contains(componentId)) {
        b.allGrouping(MetricSpout.componentId, MetricTriggerKind.streamId)
      }
    })
  }
}

def genTopo[Key, S](
  cfg: Config,
  attrs: Seq[AttrInfo],
  metricConfig: Option[MetricConfig] = None,
  ps: PartitionStructure = PartitionStructure.Skiplist,
  setStructure: SetStructure = SetStructure.SparseBitSet,
  resultDirFn: File => File,
  leafOpsConfig: Option[LeafOpsConfig] = None,
)(
  timestamp: String,
  spouts: Seq[IRichSpout],
  resultsRootDir: File,
  resultsWriter: ResultsWriter,
)(using
  SpoutTupleStream[S],
)(using Numeric[Key], Order[Key], Show[Key], ClassTag[Key]) = {
  import de.rptu.cs.dbis.collection.bitset.Bitset.given
  import de.rptu.cs.dbis.collection.initable.Initable.given
  setStructure match {
    case SetStructure.SparseBitSet => genTopoTyped[Key, S, SparseBitSet](
        cfg,
        attrs,
        metricConfig,
        ps,
        resultDirFn,
        leafOpsConfig,
      )(timestamp, spouts, resultsRootDir, resultsWriter)
    case SetStructure.RoaringBitmap =>
      genTopoTyped[Key, S, MutableRoaringBitmap](
        cfg,
        attrs,
        metricConfig,
        ps,
        resultDirFn,
        leafOpsConfig,
      )(timestamp, spouts, resultsRootDir, resultsWriter)
  }
}

private def genTopoTyped[Key, S, BS](
  cfg: Config,
  attrs: Seq[AttrInfo],
  metricConfig: Option[MetricConfig],
  ps: PartitionStructure,
  resultDirFn: File => File,
  leafOpsConfig: Option[LeafOpsConfig],
)(
  timestamp: String,
  spouts: Seq[IRichSpout],
  resultsRootDir: File,
  resultsWriter: ResultsWriter,
)(using ss: SpoutTupleStream[S])(using
  Numeric[Key],
  Order[Key],
  Show[Key],
  ClassTag[Key],
)(using ClassTag[BS], Bitset[BS], Initable[Unit, BS]) = {
  import scala.language.unsafeNulls
  val dispatcherBolt: BaseRichBolt = new Dispatcher[S](cfg)
  val joinerBolt = metricConfig.makeBolt(
    Joiner.componentId,
    new Joiner[BS](cfg, metricConfig.containsComponentId(Joiner.componentId)),
  )
  val storageBolt: BaseRichBolt = metricConfig.makeBolt(
    Storage.componentId,
    new Storage[BS](
      cfg,
      metricConfig.containsComponentId(Storage.componentId),
      (resultDirFn(resultsRootDir) / timestamp).toString,
      resultsWriter,
    ),
  )

  val builder = new TopologyBuilder()

  builder.setSpout(TriggerSpout.componentId, new TriggerSpout(cfg), 1)
  spouts
    .lazyZip(ss.componentIds)
    .foreach((sp, id) => { builder.setSpout(id, sp, 1) })

  metricConfig.foreach(c => {
    val writer =
      new MetricWriter(c.kindsMap, (resultsRootDir / "metric").toString())
    builder.setSpout(MetricSpout.componentId, c.spout)
    val writerDeclarer: BoltDeclarer = builder
      .setBolt(MetricWriter.componentId, writer)
    for (MetricKind(cid, sid, _) <- c.allMetricKinds) {
      writerDeclarer.globalGrouping(cid, sid)
    }
  })

  val dispatcherBoltDeclarer = builder
    .setBolt(Dispatcher.componentId, dispatcherBolt, cfg.dispatcherNumber)
  for (id <- ss.componentIds) { dispatcherBoltDeclarer.shuffleGrouping(id) }

  for (ai <- attrs) {
    val indexerInitCfg = cfg.indexerCfgs.get(ai.attr.name) match {
      case Some(ic) => cfg.copy(
          bufferCap = ic.bufferCap.getOrElse(cfg.bufferCap),
          partitionNum = ic.partitionNum.getOrElse(cfg.partitionNum),
          initialBufferCap = ic.initialBufferCap.getOrElse(cfg.initialBufferCap),
        )
      case None => cfg
    }
    val idxer = metricConfig.makeBolt(
      ai.indexerId,
      new Indexer[Key, BS](indexerInitCfg, ai.attr.name, ai.scanOp),
    )
    val idxerDeclarer: BoltDeclarer = builder
      .setBolt(ai.indexerId, idxer, 1)
      .globalGrouping(Dispatcher.componentId, ai.attr.indexKind.streamId)
      .globalGrouping(
        Dispatcher.componentId,
        ss.queryRelation(ai.attr).queryKind.streamId,
      )
      .globalGrouping(
        TriggerSpout.componentId,
        trigger.PartitionKind(ai.attr.name).streamId,
      )
      .globalGrouping(ai.partitionId, partition.PartitionRangeKind.streamId)
    metricConfig.activateBoltOnDemand(ai.indexerId)(idxerDeclarer)

    val partMetric = metricConfig.containsComponentId(ai.partitionId)
    val part = metricConfig.makeBolt(
      ai.partitionId,
      ps match {
        case PartitionStructure.Skiplist =>
          new Partition[Key, SkipList[Key, TupleId, BS], BS](
            indexerInitCfg,
            ai.attr.name,
            ai.scanOp,
            partMetric,
            leafOpsConfig,
          )
        case PartitionStructure.SkiplistWithoutCache =>
          new Partition[Key, SkipListWithoutCache[Key, TupleId, BS], BS](
            indexerInitCfg,
            ai.attr.name,
            ai.scanOp,
            partMetric,
            leafOpsConfig,
          )
        case PartitionStructure.RedBlackTree =>
          new Partition[Key, RedBlackTree[Key, TupleId, BS], BS](
            indexerInitCfg,
            ai.attr.name,
            ai.scanOp,
            partMetric,
            leafOpsConfig,
          )
      },
    )

    val partitionTaskNum: Int = indexerInitCfg
      .partitionTaskNum
      .getOrElse(indexerInitCfg.partitionNum)
    val partDeclarer: BoltDeclarer = builder
      .setBolt(ai.partitionId, part, indexerInitCfg.partitionNum)
      .setNumTasks(partitionTaskNum)
      .directGrouping(ai.indexerId, indexer.QueryPartitionKind.streamId)
      .directGrouping(ai.indexerId, indexer.SendPartitionKind.streamId)
      .allGrouping(Storage.componentId, storage.ExpireTimeKind.streamId)
    metricConfig.activateBoltOnDemand(ai.partitionId)(partDeclarer)
  }

  val joinerDeclarer = builder
    .setBolt(Joiner.componentId, joinerBolt, cfg.joinerNumber)
  joinerDeclarer.directGrouping(
    Dispatcher.componentId,
    dispatcher.TargetAttrValuesKind.streamId,
  )
  metricConfig.activateBoltOnDemand(Joiner.componentId)(joinerDeclarer)

  for (ai <- attrs) {
    joinerDeclarer.directGrouping(ai.indexerId, indexer.JoinQueryKind.streamId)
    joinerDeclarer
      .directGrouping(ai.partitionId, partition.PartitionJoinQueryKind.streamId)
  }

  val storageDeclarer = builder
    .setBolt(Storage.componentId, storageBolt, cfg.storageNumber)
    .directGrouping(Dispatcher.componentId, dispatcher.StorageRowKind.streamId)
    .directGrouping(Joiner.componentId, joiner.ResultsKind.streamId)
  metricConfig.activateBoltOnDemand(Storage.componentId)(storageDeclarer)

  builder.createTopology()
}
