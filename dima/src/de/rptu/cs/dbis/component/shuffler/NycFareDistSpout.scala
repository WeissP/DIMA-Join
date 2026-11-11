package de.rptu.cs.dbis.component.shuffler

import com.github.mjakubowski84.parquet4s.{ParquetReader, Path}
import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.clock.TupleClock
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.utils.latencyRecorder.LatencyRecorder
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout

import java.io.BufferedWriter
import java.util.Map as JMap

private case class NycParquetRow(
  VendorID: Long,
  tpep_pickup_datetime: String,
  tpep_dropoff_datetime: String,
  passenger_count: Option[Int],
  trip_distance: Double,
  PULocationID: Long,
  DOLocationID: Long,
  fare_amount: Double,
)

class NycFareDistSpout[E: EmitController](
  emitController: E,
  windowSize: Long,
  datasetPath: String,
  latencyPath: String,
  initClock: => TupleClock,
) extends BaseRichSpout with Serializable with StrictLogging {
  private var collector: SpoutOutputCollector             = _
  private var rowIterator: Iterator[NycParquetRow] | Null = null
  private var clock: TupleClock                           = _
  private var id: Long                                    = 0L
  @transient
  private var writer: BufferedWriter = _

  override def open(
    conf: JMap[String, AnyRef],
    context: TopologyContext,
    collector: SpoutOutputCollector,
  ): Unit = {
    this.collector = collector
    clock = initClock
    writer = LatencyRecorder.initWriter(latencyPath)
    initReader()
  }

  private def initReader(): Unit = {
    logger.info(s"Reading parquet $datasetPath")
    rowIterator = ParquetReader
      .as[NycParquetRow]
      .read(Path(datasetPath))
      .nn
      .iterator
    assert(rowIterator.nn.hasNext, s"No tuples found in $datasetPath")
    logger.info("parquet data loaded")
  }

  override def nextTuple(): Unit = {
    if (emitController.shouldEmit) {
      if (id == windowSize) {
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      if (!rowIterator.nn.hasNext) {
        logger.info("All tuples are read, repeat")
        initReader()
      }
      id += 1
      val row    = rowIterator.nn.next()
      val tripId = s"$id"
      val tuple = NycStream(
        tripId,
        clock.now,
        row.fare_amount,
        row.trip_distance,
        0L,
        System.currentTimeMillis(),
      )
      collector.emit(tuple.tupleValues, tripId)
      LatencyRecorder.record(writer, tripId)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(NycStream.fields)
  }

  override def ack(msgId: Any): Unit = {
    emitController.onAck
    LatencyRecorder.record(writer, msgId)
  }
  override def fail(msgId: Any): Unit = {
    emitController.onFail
    logger.error(s"tuple $msgId failed")
  }

  override def close(): Unit = {
    writer.flush()
    writer.close()
  }
}

object NycFareDistSpout {
  val componentId: String = "NycFareDistSpout"

  def apply[E: EmitController](
    emitController: E,
    windowSize: Long,
    datasetPath: String,
    latencyPath: String,
    cfg: Config,
  ): NycFareDistSpout[E] = {
    new NycFareDistSpout(
      emitController,
      windowSize,
      datasetPath,
      latencyPath,
      cfg.initClock,
    )
  }
}
