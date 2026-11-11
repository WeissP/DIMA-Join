package de.rptu.cs.dbis.component.shuffler

import de.rptu.cs.dbis.domain.tuples.shuffler.*
import de.rptu.cs.dbis.domain.tuples.shuffler.spo.SpoStream
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import java.util
import java.util.Map as JMap
import scala.util.Random
import com.typesafe.scalalogging.StrictLogging

class SpoSpout[E: EmitController](emitController: E, windowSize: Long)
    extends BaseRichSpout with StrictLogging {
  private var collector: SpoutOutputCollector = _
  private var id: Int                         = 0

  override def open(
    conf: JMap[String, AnyRef],
    context: TopologyContext,
    collector: SpoutOutputCollector,
  ): Unit = { this.collector = collector }

  override def nextTuple(): Unit = {
    if (emitController.shouldEmit) {
      if (id == windowSize) {
        emitController.startSliding
        logger.info("Window starts sliding")
      }
      id += 1
      collector.emit(genTuple().tupleValues, id)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(SpoStream.fields)
  }

  override def ack(msgId: Any): Unit = { emitController.onAck }
  override def fail(msgId: Any): Unit = {
    emitController.onFail
    logger.error(s"tuple $msgId failed")
  }

  def componentId: String = { "spo-spout" }

  def genTuple(): SpoStream = {
    val rOrS = Random.nextBoolean()
    if (rOrS) {
      SpoStream(
        true,
        spo.Stream(
          id,
          Random.nextInt(100000 - 1) + 1,
          Random.nextInt(100000 - 1) + 1,
          System.currentTimeMillis(),
        ), 
      )
    } else {
      SpoStream(
        false,
        spo.Stream(
          id, 
          Random.nextInt(100000 - 1) + 1,
          Random.nextInt(100000 - 1) + 1,
          System.currentTimeMillis(),
        ),
      )
    }
  }
}
