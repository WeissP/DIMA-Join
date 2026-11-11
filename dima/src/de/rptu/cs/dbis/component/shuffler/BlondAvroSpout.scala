package de.rptu.cs.dbis.component.shuffler

import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.domain.tuples.shuffler.blond.{BlondStream, Stream}
import de.rptu.cs.dbis.utils.latencyRecorder.LatencyRecorder
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import java.io.File
import java.util.Map as JMap
import java.io.BufferedWriter
import better.files.File as BFile

class BlondAvroSpout[E: EmitController](
  emitController: E,
  windowSize: Long,
  avroPath: String,
  latencyPath: String,
  measureRange: Option[(Long, Long)] = None,
) extends BaseRichSpout with Serializable with StrictLogging {
  private var collector: SpoutOutputCollector       = _
  private var reader: DataFileReader[GenericRecord] = _
  private var base: Int                             = 0
  private var measuredAcks: Long                    = 0L
  private var firstAckMs: Long                      = Long.MaxValue
  private var lastAckMs: Long                       = Long.MinValue
  private var firstEmitMs: Long                     = 0L
  @transient
  private var writer: BufferedWriter = _

  override def open(
    conf: JMap[String, AnyRef],
    context: TopologyContext,
    collector: SpoutOutputCollector,
  ): Unit = {
    this.collector = collector
    writer = LatencyRecorder.initWriter(latencyPath)
    initReader
  }

  private def initReader = {
    logger.info(s"Reading avro $avroPath")
    reader = DataFileReader(File(avroPath), GenericDatumReader())
    assert(reader.hasNext(), s"No tuples found in $avroPath")
    logger.info("avro data loaded")
  }

  override def nextTuple(): Unit = {
    if (emitController.shouldEmit) {
      val record = reader.next().nn
      val id     = base + record.get("id").asInstanceOf[java.lang.Long].toInt
      if (measureRange.exists((from, _) => id.toLong == from)) {
        firstEmitMs = System.currentTimeMillis()
      }
      if (id.toLong == windowSize) {
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      val isStreamR = record
        .get("isStreamR")
        .asInstanceOf[java.lang.Boolean]
        .booleanValue()
      val power = record.get("power").asInstanceOf[java.lang.Long].toInt
      val cool  = record.get("cool").asInstanceOf[java.lang.Long].toInt
      val tuple = BlondStream(
        isStreamR,
        Stream(id, power, cool, System.currentTimeMillis()),
      )
      collector.emit(tuple.tupleValues, id)
      LatencyRecorder.record(writer, id)
      if (!reader.hasNext) {
        logger.info("All tuples are read, repeat")
        initReader
        base = base + id
      }
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(BlondStream.fields)
  }

  override def ack(msgId: Any): Unit = {
    emitController.onAck
    LatencyRecorder.record(writer, msgId)
    measureRange.foreach { (from, to) =>
      val id = msgId.asInstanceOf[Int].toLong
      if (id >= from && id < to) {
        val now = System.currentTimeMillis()
        measuredAcks += 1
        firstAckMs = math.min(firstAckMs, now)
        lastAckMs = math.max(lastAckMs, now)
        if (measuredAcks == to - from) { writeMeasurement(from, to) }
      }
    }
  }

  private def writeMeasurement(from: Long, to: Long): Unit = {
    val seconds    = (lastAckMs - firstAckMs) / 1000.0
    val throughput = (to - from) / seconds
    val json =
      s"""{"from": $from, "to": $to, "tuples": ${to - from}, "firstEmitMs": $firstEmitMs, "firstAckMs": $firstAckMs, "lastAckMs": $lastAckMs, "seconds": $seconds, "throughput": $throughput}"""
    logger.info(s"Measured range [$from, $to): $json")
    BFile(latencyPath.stripSuffix(".csv") + ".measure.json").writeText(json)
  }
  override def fail(msgId: Any): Unit = {
    emitController.onFail
    logger.error(s"tuple $msgId failed")
  }

  override def close(): Unit = {
    writer.flush()
    writer.close()
  }

  val componentId: String = "blond-spout"
}
