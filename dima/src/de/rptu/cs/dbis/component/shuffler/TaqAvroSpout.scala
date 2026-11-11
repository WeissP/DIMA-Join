package de.rptu.cs.dbis.component.shuffler

import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.domain.tuples.shuffler.taq.{TaqStream, Stream}
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

class TaqAvroSpout[E: EmitController](
  emitController: E,
  windowSize: Long,
  avroPath: String,
  latencyPath: String,
) extends BaseRichSpout with Serializable with StrictLogging {
  private var collector: SpoutOutputCollector       = _
  private var reader: DataFileReader[GenericRecord] = _
  private val startTs: Long                         = System.currentTimeMillis()
  private var minTs: Long                           = 0
  private var base: Int                             = 0
  private var nextId: Long                          = 0L
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

  override def nextTuple(): Unit = {
    if (reader.hasNext && emitController.shouldEmit) {
      val record = reader.next().nn
      val id     = base + nextId
      nextId += 1
      val tradeId   = record.get("trade_id").asInstanceOf[java.lang.Long].toLong
      val isStreamR = tradeId != -1L
      val price = record
        .get("price")
        .asInstanceOf[java.lang.Double]
        .doubleValue()
      val qty = record.get("size").asInstanceOf[java.lang.Double].doubleValue()
      val timestamp = record
        .get("timestamp")
        .asInstanceOf[java.lang.Long]
        .toLong
      val arrivalTime = System.currentTimeMillis()
      val tuple =
        TaqStream(isStreamR, Stream(id, price, qty, timestamp, arrivalTime))
      if (minTs == 0) { minTs = timestamp }
      if (!startSliding && timestamp >= minTs + windowSize) {
        startSliding = true
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      collector.emit(tuple.tupleValues, id)
      LatencyRecorder.record(writer, id)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(TaqStream.fields)
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

  val componentId: String = "taq-spout"
}
