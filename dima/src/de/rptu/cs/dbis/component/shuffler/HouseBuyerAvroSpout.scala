package de.rptu.cs.dbis.component.shuffler

import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer.{
  HouseBuyerStream,
  Stream,
}
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

class HouseBuyerAvroSpout[E: EmitController](
  emitController: E,
  windowSize: Long,
  avroPath: String,
  latencyPath: String,
) extends BaseRichSpout with Serializable with StrictLogging {
  private var collector: SpoutOutputCollector       = _
  private var reader: DataFileReader[GenericRecord] = _
  private var startSliding: Boolean                 = false
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

  private def toFloat(obj: AnyRef): Float = obj match {
    case l: java.lang.Long    => l.floatValue()
    case i: java.lang.Integer => i.floatValue()
    case f: java.lang.Float   => f.floatValue()
    case d: java.lang.Double  => d.floatValue()
    case _ =>
      throw new RuntimeException(s"Unsupported numeric type: ${obj.getClass}")
  }

  private def qualityValue(record: GenericRecord, name: String): Float = {
    if (record.getSchema.nn.getField(name) == null) { 0.0f }
    else { Option(record.get(name)).fold(0.0f)(value => toFloat(value.nn)) }
  }

  override def nextTuple(): Unit = {
    if (reader.hasNext && emitController.shouldEmit) {
      val record = reader.next().nn
      val id     = record.get("id").nn.asInstanceOf[java.lang.Long].longValue()

      val isStreamR = record
        .get("isStreamR")
        .nn
        .asInstanceOf[java.lang.Boolean]
        .booleanValue()

      val price = toFloat(record.get("A").nn)
      val area  = toFloat(record.get("B").nn)
      val q1    = qualityValue(record, "quality1")
      val q2    = qualityValue(record, "quality2")
      val q3    = qualityValue(record, "quality3")

      val tuple = HouseBuyerStream(
        isStreamR,
        Stream(id, price, area, q1, q2, q3, System.currentTimeMillis()),
      )
      if (!startSliding && id >= windowSize) {
        startSliding = true
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      collector.emit(tuple.tupleValues, id)
      LatencyRecorder.record(writer, id)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(HouseBuyerStream.fields)
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
    reader.close()
    writer.flush()
    writer.close()
  }

  val componentId: String = "housebuyer-spout"
}
