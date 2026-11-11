package de.rptu.cs.dbis.component.trigger

import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.domain.tuples.trigger.*
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import scala.collection.immutable.ArraySeq
import com.typesafe.scalalogging.StrictLogging

class TriggerSpout(cfg: Config) extends BaseRichSpout with StrictLogging {
  private val partitionKinds: ArraySeq[PartitionKind] = cfg
    .attrNames
    .map(PartitionKind(_))
  private var collector: SpoutOutputCollector = _
  private val partitionMessages = {
    Array.from(
      cfg
        .attrNames
        .map(attr =>
          (PartitionKind(attr), Partition(0, PartitionAction.Send, attr)),
        ),
    )
  }

  private var init = false

  override def open(
    conf: java.util.Map[String, AnyRef],
    context: TopologyContext,
    collector: SpoutOutputCollector,
  ): Unit = { this.collector = collector }

  override def nextTuple(): Unit = {
    if (init) { Thread.sleep(1) }
    else {
      partitionMessages
        .zipWithIndex
        .foreach { case ((triggerKind, trigger), attrIdx) =>
          collector.emit(
            triggerKind.streamId,
            trigger.tupleValues,
            (attrIdx, trigger.clock),
          )
        }

      init = true
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    partitionMessages.map(_._1).foreach { _.declare(declarer) }
  }

  override def ack(msgId: Object): Unit = {
    val attrIdx = msgId.asInstanceOf[(Int, Int)]._1
    val (kind, Partition(clock, action, attrName)) = partitionMessages(attrIdx)
    val msg = Partition(clock + 1, action.switch, attrName)
    partitionMessages(attrIdx) = (kind, msg)
    logger.debug(s"sending trigger: $msg")
    collector.emit(kind.streamId, msg.tupleValues, (attrIdx, clock + 1))
  }

  override def fail(msgId: Object): Unit = {
    logger.debug(s"trigger $msgId failed")
    val attrIdx     = msgId.asInstanceOf[(Int, Int)]._1
    val (kind, msg) = partitionMessages(attrIdx)
    collector.emit(kind.streamId, msg.tupleValues, (attrIdx, msg.clock))
  }
}

object TriggerSpout {
  val componentId: String = "TriggerSpout"
}
