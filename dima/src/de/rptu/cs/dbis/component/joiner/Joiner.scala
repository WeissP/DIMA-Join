package de.rptu.cs.dbis.component.joiner

import io.circe.Encoder
import de.rptu.cs.dbis.domain.tuples.metric.MetricKind
import de.rptu.cs.dbis.domain.tuples.metric.MetricMessage
import io.circe.generic.auto.*
import cats.syntax.all.*
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertEqIfDbg
import de.rptu.cs.dbis.domain.tuples.*
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.*
import de.rptu.cs.dbis.collection.bitset.Bitset
import scala.reflect.ClassTag
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.component.storage.Storage
import com.typesafe.scalalogging.StrictLogging

class Joiner[BS](cfg: Config, enableMetric: Boolean = false)(using
  ClassTag[BS],
  Bitset[BS],
  Initable[Unit, BS],
) extends BaseRichBolt with StrictLogging {

  private var q: AggQueue[BS]                    = _
  private var collector: OutputCollector         = _
  private var storageTasks: ArraySeq[Integer]    = _
  private var joinerIdx: Int                     = _
  private var dispatcherIdx: Int                 = _
  private var metricPayload: JoinerMetricPayload = _

  override def prepare(
    topoConf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    this.collector = collector
    this.joinerIdx = context.getThisTaskIndex()
    this.dispatcherIdx = cfg.joinerIdxToDispatcherIdx(this.joinerIdx)
    storageTasks = ArraySeq.from(
      context
        .getComponentTasks(Storage.componentId)
        .nn
        .asScala
        .take(cfg.storageNumber),
    )
    assertEqIfDbg(
      s"[Joiner $joinerIdx] Storage number mismatch",
      cfg.storageNumber,
      storageTasks.length,
    )
    this.q = AggQueue(cfg, dispatcherIdx, joinerIdx, storageTasks, collector)
    if (enableMetric) { metricPayload = JoinerMetricPayload.init }
  }

  override def execute(tuple: Tuple): Unit = {
    tuple.kind match {
      case TupleKind.Metric => {
        val trigger = metric.MetricTrigger.fromTuple(tuple)
        metricPayload = metricPayload
          .copy(joinerIdx = joinerIdx, aggQueueLength = q.length)
        if (q.length > 30) {
          metricPayload = metricPayload.copy(queueStatus = q.status)
        }
        collector.emit(
          Joiner.metricKind.streamId,
          MetricMessage(trigger, metricPayload).tupleValues,
        )
        metricPayload = JoinerMetricPayload.init
      }
      case TupleKind.PartitionResult => {
        if (enableMetric) {
          metricPayload = metricPayload.copy(partitionResultDur =
            metricPayload.partitionResultDur - System.currentTimeMillis(),
          )
        }
        val msg = partition.PartitionJoinQuery.fromTuple[BS](tuple)
        logger.debug(
          show"[Joiner $joinerIdx] Received PartitionResult for tuple ${msg.tupleId.localId} and attr ${msg.attrName}",
        )
        logger.trace(
          show"[Joiner $joinerIdx] Received PartitionResult for tuple ${msg.tupleId.localId} and attr ${msg
              .attrName}, found tuples: ${msg.tuples}",
        )
        q.updateAgg(
          msg.tupleId,
          (agg => {
            agg.operateAttr(msg.attrName)(ar => {
              ar.insertPartitionResults(msg.tuples)
            })
            if (enableMetric && agg.result.isDefined) {
              metricPayload = metricPayload
                .copy(partitionCount = metricPayload.partitionCount + 1)
            }
          }),
        )
        if (enableMetric) {
          metricPayload = metricPayload.copy(
            partitionResultDur = metricPayload.partitionResultDur +
              System.currentTimeMillis(),
            latestPartitionLID = msg.tupleId.localId,
            lidOrder = metricPayload
              .lidOrder
              .append(
                (msg.tupleId.localId, "partition", System.currentTimeMillis()),
              ),
          )
        }
      }
      case TupleKind.IndexerResult => {
        if (enableMetric) {
          metricPayload = metricPayload.copy(indexerResultDur =
            metricPayload.indexerResultDur - System.currentTimeMillis(),
          )
        }
        val msg = indexer.JoinQuery.fromTuple[BS](tuple)
        logger.debug(
          show"[Joiner $joinerIdx] Received IndexerResult for tuple ${msg.tupleId.localId} and attr ${msg.attrName}",
        )
        logger.trace(
          show"[Joiner $joinerIdx] Received IndexerResult for tuple ${msg.tupleId.localId} and attr ${msg
              .attrName}, found tuples: ${msg.tuples}",
        )
        q.updateAgg(
          msg.tupleId,
          (agg => {
            agg.setTimestamp(msg.timestamp)
            agg.operateAttr(msg.attrName)(ar => {
              ar.insertIndexerResults(msg.partitionNum, msg.tuples)
            })
            if (enableMetric && agg.result.isDefined) {
              metricPayload = metricPayload
                .copy(indexerCount = metricPayload.indexerCount + 1)
            }
          }),
        )
        if (enableMetric) {
          metricPayload = metricPayload.copy(
            indexerResultDur = metricPayload.indexerResultDur +
              System.currentTimeMillis(),
            latestIndexerLID = msg.tupleId.localId,
            lidOrder = metricPayload
              .lidOrder
              .append(
                (msg.tupleId.localId, "indexer", System.currentTimeMillis()),
              ),
          )
        }
      }
      case TupleKind.TargetAttrValues => {
        if (enableMetric) {
          metricPayload = metricPayload.copy(targetAttrValuesDur =
            metricPayload.targetAttrValuesDur - System.currentTimeMillis(),
          )
        }
        val msg = dispatcher.TargetAttrValues.fromTuple(tuple)
        logger.debug(
          show"[Joiner $joinerIdx] Received TargetAttrValues for tuple ${msg.tupleId}",
        )
        q.updateAgg(
          msg.tupleId,
          (agg => {
            agg.setTargetValues(msg.attrValues)
            agg.setAnchorTuple(tuple)
            if (enableMetric && agg.result.isDefined) {
              metricPayload = metricPayload
                .copy(dispatcherCount = metricPayload.dispatcherCount + 1)
            }
          }),
        )
        if (enableMetric) {
          metricPayload = metricPayload.copy(
            targetAttrValuesDur = metricPayload.targetAttrValuesDur +
              System.currentTimeMillis(),
            dispatcherCount = metricPayload.dispatcherCount + 1,
            latestDispatcherLID = msg.tupleId.localId,
            lidOrder = metricPayload
              .lidOrder
              .append(
                (msg.tupleId.localId, "dispatcher", System.currentTimeMillis()),
              ),
          )
        }
      }
    }

    if (enableMetric) {
      metricPayload = metricPayload.copy(tryProcessDur =
        metricPayload.tryProcessDur - System.currentTimeMillis(),
      )
    }
    q.tryProcess
    if (enableMetric) {
      metricPayload = metricPayload.copy(tryProcessDur =
        metricPayload.tryProcessDur + System.currentTimeMillis(),
      )
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    joiner.ResultsKind.declare(declarer)
    if (enableMetric) { Joiner.metricKind.declare(declarer) }
  }
}

object Joiner {
  def componentId: String = "joiner"
  val metricKind: MetricKind =
    MetricKind(componentId, "joiner-metric", "joiner")
}

private enum TupleKind {
  case IndexerResult
  case TargetAttrValues
  case PartitionResult
  case Metric
}

extension (t: Tuple) {
  private def kind: TupleKind = {
    val id = t.getSourceStreamId().nn
    if (id === indexer.JoinQueryKind.streamId) { TupleKind.IndexerResult }
    else if (id === partition.PartitionJoinQueryKind.streamId) {
      TupleKind.PartitionResult
    } else if (id === dispatcher.TargetAttrValuesKind.streamId) {
      TupleKind.TargetAttrValues
    } else if (id === "metric-stream") { TupleKind.Metric }
    else { throw new RuntimeException(s"unknown tuple source: $id") }
  }
}
