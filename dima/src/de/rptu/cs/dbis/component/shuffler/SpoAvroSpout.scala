package de.rptu.cs.dbis.component.shuffler

import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.domain.tuples.shuffler.spo.{SpoStream, Stream}
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

class SpoAvroSpout[E: EmitController](
  emitController: E,
  windowSize: Long,
  avroPath: String,
  latencyPath: String,
) extends BaseRichSpout with Serializable with StrictLogging {
  private var collector: SpoutOutputCollector       = _
  private var reader: DataFileReader[GenericRecord] = _
  private val startTs: Long                         = System.currentTimeMillis()
  private var base: Int                             = 0
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
      if (id.toLong == windowSize) {
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      val isStreamR = record
        .get("isStreamR")
        .asInstanceOf[java.lang.Boolean]
        .booleanValue()
      val a = record.get("A").asInstanceOf[java.lang.Long].toInt
      val b = record.get("B").asInstanceOf[java.lang.Long].toInt
      val tuple =
        SpoStream(isStreamR, Stream(id, a, b, System.currentTimeMillis()))
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
    declarer.declare(SpoStream.fields)
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

  val componentId: String = "spo-spout"
}
