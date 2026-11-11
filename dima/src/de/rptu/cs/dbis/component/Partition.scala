package de.rptu.cs.dbis.component.partition

import cats.kernel.Order
import cats.syntax.all.*
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertEqIfDbg
import de.rptu.cs.dbis.domain.{Attr, CollectionStatistic, TupleId}
import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.tuples.metric.MetricMessage
import io.circe.Encoder
import io.circe.generic.auto.*
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import com.typesafe.scalalogging.StrictLogging
import scala.collection
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import de.rptu.cs.dbis.domain.tuples.metric.MetricKind
import de.rptu.cs.dbis.collection.scanmap.ScanMapClass
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.component.joiner.Joiner
import cats.Show
import de.rptu.cs.dbis.topology.helper.AttrInfo
import de.rptu.cs.dbis.domain.tuples.attribute.Attribute

class Partition[Key, ScanMap, BS](
  cfg: Config,
  attrName: String,
  scanOp: ScanOperator,
  enableMetric: Boolean = false,
  leafOpsConfig: Option[LeafOpsConfig] = None,
)(using initScanMap: Initable[(Config, ScanOperator), ScanMap])(using
  Numeric[Key],
  Show[Key],
  Order[Key],
  ClassTag[Key],
  ClassTag[BS],
  Bitset[BS],
  Initable[Unit, BS],
  ScanMapClass[ScanMap, Key, TupleId, BS],
) extends BaseRichBolt with Serializable with StrictLogging {
  private var collector: OutputCollector     = _
  private var kvmap: ScanMap                 = _
  private val minTuples                      = new MinTuples(cfg)
  var partitionIdx: Int                      = -1
  var partitionId: Int                       = -1
  private var joinerTasks: ArraySeq[Integer] = _

  private var metricPayload: PartitionMetricPayload = _
  private var leafOps: Option[LeafOpsWriter]       = None

  lazy val partitionMetricKind: MetricKind =
    AttrInfo(Attribute(attrName), scanOp).partitionMetricKind

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    this.collector = collector
    kvmap = initScanMap.init(cfg, scanOp)
    partitionIdx = context.getThisTaskIndex()
    partitionId = context.getThisTaskId
    joinerTasks = ArraySeq.from(
      context
        .getComponentTasks(Joiner.componentId)
        .nn
        .asScala
        .take(cfg.joinerNumber),
    )
    logger.info(
      show"[Partition $partitionIdx] joinerTasks: ${joinerTasks.map(_.toInt).toList}",
    )
    assertEqIfDbg(
      "unmatched joinerNumber and joinerTasks",
      cfg.joinerNumber,
      joinerTasks.length,
    )
    if (enableMetric) { metricPayload = initPayload }
    leafOps = leafOpsConfig
      .filter(_.attrName == attrName)
      .map(tc => {
        require(
          cfg.dispatcherNumber == 1,
          "leaf ops recording supports only one dispatcher",
        )
        new LeafOpsWriter(tc, partitionIdx)
      })
  }

  override def cleanup(): Unit = { leafOps.foreach(_.close()) }

  private def initPayload: PartitionMetricPayload = {
    PartitionMetricPayload(
      this.attrName,
      this.partitionId,
      this.partitionIdx,
      kvmap.collectionStatistic,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    )
  }

  override def execute(tuple: Tuple): Unit = {
    tuple.kind match {
      case TupleKind.Store => {
        if (enableMetric) {
          metricPayload = metricPayload.copy(storeDur =
            metricPayload.storeDur - System.currentTimeMillis(),
          )
        }
        val sp = indexer.SendPartition.fromTuple[Key](tuple)
        leafOps.foreach(_.store(sp.values.view.map(a => {
          (summon[Numeric[Key]].toInt(a.value), a.tupleId.localId)
        })))
        kvmap.insertSortedPairs(sp.values)
        collector.ack(tuple)
        if (enableMetric) {
          metricPayload = metricPayload.copy(storeDur =
            metricPayload.storeDur + System.currentTimeMillis(),
          )
        }
      }
      case TupleKind.Query => {
        val q = indexer.QueryPartition.fromTuple[Key](tuple)
        leafOps.foreach(_.query(summon[Numeric[Key]].toInt(q.key), q.tupleId.localId))
        if (enableMetric) {
          metricPayload = metricPayload.copy(queryDur =
            metricPayload.queryDur - System.currentTimeMillis(),
          )
          val isFull = scanOp match {
            case ScanOperator.Greater =>
              kvmap.minKey.map(q.key <= _).getOrElse(false)
            case ScanOperator.Less =>
              kvmap.maxKey.map(q.key >= _).getOrElse(false)
          }
          if (isFull) {
            metricPayload = metricPayload.copy(
              fullRangeCount = metricPayload.fullRangeCount + 1,
              fullRangeDur = metricPayload.fullRangeDur -
                System.currentTimeMillis(),
            )
          } else {
            metricPayload = metricPayload.copy(
              scanCount = metricPayload.scanCount + 1,
              scanDur = metricPayload.scanDur - System.currentTimeMillis(),
            )
          }
          val foundTupleIds = kvmap.scan(q.key)
          if (isFull) {
            metricPayload = metricPayload.copy(
              fullRangeDur = metricPayload.fullRangeDur +
                System.currentTimeMillis(),
              fullRangeEmitDur = metricPayload.fullRangeEmitDur -
                System.currentTimeMillis(),
            )
          } else { 
            metricPayload = metricPayload.copy(
              scanDur = metricPayload.scanDur + System.currentTimeMillis(),
              scanEmitDur = metricPayload.scanEmitDur -
                System.currentTimeMillis(),
            )
          }
          collector.emitDirect( 
            joinerTasks(cfg.getJoinerIdx(q.tupleId)),
            partition.PartitionJoinQueryKind.streamId,
            tuple,
            partition
              .PartitionJoinQuery
              .tupleValues(q.tupleId, attrName, foundTupleIds.toBytes),
          )
          if (isFull) {
            metricPayload = metricPayload.copy(fullRangeEmitDur =
              metricPayload.fullRangeEmitDur + System.currentTimeMillis(),
            )
          } else {
            metricPayload = metricPayload.copy(scanEmitDur =
              metricPayload.scanEmitDur + System.currentTimeMillis(),
            )
          }
          metricPayload = metricPayload.copy(queryDur =
            metricPayload.queryDur + System.currentTimeMillis(),
          )
        } else {
          val foundTupleIds = kvmap.scan(q.key)
          logger.debug {
            val idx    = cfg.getJoinerIdx(q.tupleId)
            val joiner = joinerTasks(idx).toInt
            show"[Partition $partitionIdx] sending results to joiner $joiner of index $idx"
          }
          collector.emitDirect(
            joinerTasks(cfg.getJoinerIdx(q.tupleId)),
            partition.PartitionJoinQueryKind.streamId,
            tuple,
            partition
              .PartitionJoinQuery
              .tupleValues(q.tupleId, attrName, foundTupleIds.toBytes),
          )
        }
      }
      case TupleKind.Expire => {
        if (enableMetric) {
          metricPayload = metricPayload.copy(expireDur =
            metricPayload.expireDur - System.currentTimeMillis(),
          )
        }
        val msg = storage.ExpireTuples.fromTuple(tuple)
        minTuples.put(msg.minTuple)
        minTuples.tryProcess((minLocalIds, isExpired) => {
          leafOps.foreach(_.expire(minLocalIds(0)))
          val intvBefore = kvmap.keyInterval
          kvmap.filterValues(isExpired.andThen(!_))
          val intvAfter = kvmap.keyInterval

          if (intvBefore != intvAfter) {
            collector.emit(
              partition.PartitionRangeKind.streamId,
              partition.PartitionRange(partitionIdx, intvAfter).tupleValues,
            )
          }
        })
        if (enableMetric) {
          metricPayload = metricPayload.copy(expireDur =
            metricPayload.expireDur + System.currentTimeMillis(),
          )
        }
      }
      case TupleKind.MetricTrigger => {
        val trigger = metric.MetricTrigger.fromTuple(tuple)
        val payload = metricPayload
          .copy(scanmapInfo = kvmap.collectionStatistic)
        val metricMessage = MetricMessage(trigger, payload)
        collector.emit(partitionMetricKind.streamId, metricMessage.tupleValues)
        metricPayload = initPayload
      }
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    partition.PartitionJoinQueryKind.declare(declarer)
    partition.PartitionRangeKind.declare(declarer)
    partitionMetricKind.declare(declarer)
  }
}

object Partition {
  def componentId(attrName: String): String = s"Partition-$attrName"
}

class MinTuples(cfg: Config) extends Serializable with StrictLogging {
  // The minimum non-expired local IDs of each dispatcher ID
  private var minTuples         = Map[Int, Int]()
  private var processedNum: Int = 0

  def put(tid: TupleId): Unit = {
    processedNum += 1
    minTuples = minTuples.updatedWith(tid.dispatcherIdx) {
      case Some(current) => Some(current.max(tid.localId))
      case None          => Some(tid.localId)
    }
  }

  def tryProcess(p: (Map[Int, Int], TupleId => Boolean) => Unit): Unit = {
    if (
      processedNum >= cfg.totalSampleStorageNum &&
      minTuples.size === cfg.dispatcherNumber
    ) {
      val minLocalIDs = minTuples
        .view
        .mapValues(lid => lid - cfg.storagePerDispatcher + 1)
        .toMap
      p(minLocalIDs, { case TupleId(d, l) => minLocalIDs(d) > l })
      BuildEnv.ifDbg {
        val minTuplesMap = minTuples.toMap
        logger.trace(show"expired all tuples regarding $minTuplesMap")
      }
      minTuples = Map()
      processedNum = 0
    }
  }
}

private enum TupleKind {
  case Store
  case Query
  case Expire
  case MetricTrigger
}

extension (t: Tuple) {
  private def kind: TupleKind = {
    val store         = indexer.SendPartitionKind.streamId
    val query         = indexer.QueryPartitionKind.streamId
    val expire        = storage.ExpireTimeKind.streamId
    val metricTrigger = metric.MetricTriggerKind.streamId
    t.getSourceStreamId() match {
      case `store`         => TupleKind.Store
      case `query`         => TupleKind.Query
      case `expire`        => TupleKind.Expire
      case `metricTrigger` => TupleKind.MetricTrigger
      case s => throw new RuntimeException(
          s"Partition component received tuple from unknown source stream: $s",
        )
    }
  }
}

private case class PartitionMetricPayload(
  attrName: String,
  partitionId: Int,
  partitionIdx: Int,
  scanmapInfo: CollectionStatistic,
  scanCount: Int,
  fullRangeCount: Int,
  storeDur: Long,
  queryDur: Long,
  expireDur: Long,
  scanDur: Long,
  fullRangeDur: Long,
  scanEmitDur: Long,
  fullRangeEmitDur: Long,
)
