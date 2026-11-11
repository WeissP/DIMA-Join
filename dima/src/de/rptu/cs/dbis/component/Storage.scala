package de.rptu.cs.dbis.component.storage

import io.circe.Encoder
import io.circe.generic.auto.*
import cats.syntax.all.*
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.*
import de.rptu.cs.dbis.utils.debug.BuildEnv.{
  assertIfDbg,
  assertNoMsgsIfDbg,
}
import de.rptu.cs.dbis.domain.{TimestampRecords, TupleId}
import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.tuples.dispatcher.StorageRow
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Tuple
import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ArrayDeque
import org.apache.storm.topology.base.BaseRichBolt
import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.tuples.metric.MetricKind
import de.rptu.cs.dbis.domain.tuples.metric.MetricMessage
import better.files.*
import de.rptu.cs.dbis.utils.resultsWriter.ResultsWriter
import java.io.{BufferedWriter, DataOutputStream}
import de.rptu.cs.dbis.collection.initable.Initable
import scala.reflect.ClassTag

class Storage[BS](
  cfg: Config,
  enableMetric: Boolean = false,
  resultDir: String,
  resultsWriter: ResultsWriter,
)(using Bitset[BS], Initable[Unit, BS], ClassTag[BS])
    extends BaseRichBolt with StrictLogging {
  var storageId: Int                     = -1
  var storageIdx: Int                    = -1
  var dispatcherIdx: Int                 = _
  private var collector: OutputCollector = _
  val timestampRecords: TimestampRecords = TimestampRecords
    .ofCap(cfg.joinerNumber)
  val rows: ArrayDeque[StorageRow] = mutable.ArrayDeque.empty[StorageRow]
  var isSampled: Boolean           = _
  var expireClock: Int             = 0
  var lastSentLocalId: Int         = 0
  var durationMetric: StorageDurationMetricPayload = _
  var initRowStr: String                           = _
  @transient
  private var writer: BufferedWriter = _
  @transient
  private var idsOut: Option[DataOutputStream] = None

  private def validateQueue(q: => ArrayDeque[StorageRow]): Seq[String] = {
    assert(BuildEnv.DebugEnv)
    Option
      .when(q.size > 1) {
        val step = cfg.storagePerDispatcher
        q.sliding(2)
          .flatMap {
            case ArrayDeque(prev, next)
                if (next.localId != prev.localId + step) =>
              Some(
                s"StorageRow in queue must be spaced by $step, but found ${prev
                    .localId} and ${next.localId}",
              )
            case _ => none
          }
      }
      .toSeq
      .flatten
      .toSeq
  }

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    this.collector = collector
    storageId = context.getThisTaskId()
    storageIdx = context.getThisTaskIndex()
    dispatcherIdx = cfg.storageIdxToDispatcherIdx(storageIdx)
    isSampled = cfg.isSampledStorage(storageIdx)
    initRowStr = s"$storageIdx,$dispatcherIdx,"
    val p = File(resultDir) / s"storage-$storageIdx.csv"
    writer = resultsWriter.initFile(p)
    idsOut = resultsWriter
      .initIdsFile(File(resultDir) / s"storage-$storageIdx.ids.bin")
    if (enableMetric) {
      durationMetric =
        StorageDurationMetricPayload(storageIdx, storageId, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  override def execute(tuple: Tuple): Unit = {
    tuple.kind match {
      case TupleKind.Metric => {
        val trigger = metric.MetricTrigger.fromTuple(tuple)
        collector.emit(
          Storage.durationMetricKind.streamId,
          MetricMessage(trigger, durationMetric).tupleValues,
        )
        durationMetric = StorageDurationMetricPayload(
          storageIdx,
          storageId,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
        )
      }
      case TupleKind.Store => {
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(store = durationMetric.store - System.currentTimeMillis())
        }
        rows.append(dispatcher.StorageRow.fromTuple(tuple))
        assertNoMsgsIfDbg(validateQueue(rows))
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(store = durationMetric.store + System.currentTimeMillis())
        }
      }
      case TupleKind.Join => {
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(query = durationMetric.query - System.currentTimeMillis())
        }

        if (enableMetric) {
          durationMetric = durationMetric.copy(decodeResults =
            durationMetric.decodeResults - System.currentTimeMillis(),
          )
        }
        val jr = joiner.Results.fromTuple[BS](tuple)
        if (enableMetric) {
          durationMetric = durationMetric.copy(decodeResults =
            durationMetric.decodeResults + System.currentTimeMillis(),
          )
        }
        logger.debug(show"[Storage $storageIdx] received target tuple ${jr
            .targetTupleId} from joiner ${jr.joinerIdx}")
        if (enableMetric) {
          durationMetric = durationMetric.copy(updateTimestamp =
            durationMetric.updateTimestamp - System.currentTimeMillis(),
          )
        }
        if (timestampRecords.put(jr.joinerIdx, jr.timestamp)) {
          logger.debug(show"update min time to ${timestampRecords
              .currentMin} because of new jr.timestamp ${jr.timestamp}")
          assertIfDbg(
            show"jr.timestamp ${jr.timestamp} must be at least ${timestampRecords.currentMin}",
            jr.timestamp >= timestampRecords.currentMin,
          )
          val m = timestampRecords.currentMin - cfg.windowSize
          logger
            .debug(show"Storage-${storageIdx} expire all tuples older than $m")
          while (rows.headOption.exists(_.timestamp < m)) {
            rows.removeHead(false)
          }
          if (isSampled) {
            rows
              .headOption
              .foreach(r => {
                if (r.localId - lastSentLocalId >= cfg.expireInterval) {
                  collector.emit(
                    storage.ExpireTimeKind.streamId,
                    storage
                      .ExpireTuples(TupleId(dispatcherIdx, r.localId))
                      .tupleValues,
                  )
                  lastSentLocalId = r.localId
                }
              })
          }
        }
        if (enableMetric) {
          durationMetric = durationMetric.copy(updateTimestamp =
            durationMetric.updateTimestamp + System.currentTimeMillis(),
          )
        }

        if (!rows.isEmpty) {
          if (enableMetric) {
            durationMetric = durationMetric.copy(lookupResults =
              durationMetric.lookupResults - System.currentTimeMillis(),
            )
          }
          val minLid  = rows.head.localId
          val maxLid  = rows.last.localId
          val headPos = cfg.getStorePostion(storageIdx, minLid)
          // fine with negative numbers
          val minTs   = jr.timestamp - cfg.windowSize
          val lastPos = rows.size - 1

          def processFoundIds(processRow: StorageRow => Unit) = {
            def iter(processRow: (StorageRow) => Unit)(absPos: Int): Boolean = {
              val pos = absPos - headPos
              if (pos <= 0) { return true }
              if (pos > lastPos) { return false }

              val row = rows(pos)

              if (row.timestamp < minTs) { return true }
              if (row.timestamp > jr.timestamp) { return false }

              processRow(row)

              true
            }

            jr.positions.foreach(iter(processRow))
          }

          if (enableMetric) {
            durationMetric = durationMetric.copy(lookupResults =
              durationMetric.lookupResults + System.currentTimeMillis(),
            )
          }

          if (enableMetric) {
            durationMetric = durationMetric.copy(emitResults =
              durationMetric.emitResults - System.currentTimeMillis(),
            )
          }
          resultsWriter
            .writeRow(writer, idsOut, initRowStr, jr, processFoundIds)
          if (enableMetric) {
            durationMetric = durationMetric.copy(emitResults =
              durationMetric.emitResults + System.currentTimeMillis(),
            )
          }
        } else {
          resultsWriter.writeRow(writer, idsOut, initRowStr, jr, _ => ())
        }
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(ack = durationMetric.ack - System.currentTimeMillis())
        }
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(ack = durationMetric.ack + System.currentTimeMillis())
        }
        collector.ack(tuple)
        if (enableMetric) {
          durationMetric = durationMetric
            .copy(query = durationMetric.query + System.currentTimeMillis())
        }
      }
    }
  }

  override def cleanup(): Unit = {
    writer.flush()
    writer.close()
    idsOut.foreach(_.close())
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    storage.ExpireTimeKind.declare(declarer)
    if (enableMetric) { Storage.durationMetricKind.declare(declarer) }
  }
}

object Storage {
  val componentId: String = "Storage"
  val durationMetricKind: MetricKind =
    MetricKind(componentId, "storage-duration-metric", "storage-duration")
}

private enum TupleKind {
  case Store
  case Join
  case Metric
}

extension (t: Tuple) {
  private def kind: TupleKind = t.getSourceStreamId() match {
    case "storage-row"   => TupleKind.Store
    case "join-results"  => TupleKind.Join
    case "metric-stream" => TupleKind.Metric
    case s => throw new RuntimeException(s"unknown tuple source: $s")
  }
}

case class StorageDurationMetricPayload(
  storageIdx: Int,
  storageId: Int,
  query: Long,
  store: Long,
  decodeResults: Long,
  updateTimestamp: Long,
  lookupResults: Long,
  emitResults: Long,
  ack: Long,
)
