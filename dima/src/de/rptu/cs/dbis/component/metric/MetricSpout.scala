package de.rptu.cs.dbis.component.metric

import de.rptu.cs.dbis.domain.tuples.metric.{MetricTrigger, MetricTriggerKind}
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.utils.Utils

/** Emits a tuple containing a monotonically increasing ID and the current timestamp at regular intervals.
  *
  * @param intervalMs The interval in milliseconds between emitting tuples.
  */
class MetricSpout(intervalMs: Long = 500) extends BaseRichSpout {
  private var collector: SpoutOutputCollector = _
  private var idCounter: Long                 = 0L

  override def open(
    conf: java.util.Map[String, AnyRef],
    context: TopologyContext,
    collector: SpoutOutputCollector,
  ): Unit = { this.collector = collector }

  override def nextTuple(): Unit = {
    val id        = idCounter
    val timestamp = System.currentTimeMillis()
    val trigger   = MetricTrigger(id, timestamp)
    collector.emit(MetricTriggerKind.streamId, trigger.tupleValues)
    Utils.sleep(intervalMs)
    idCounter += 1
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    MetricTriggerKind.declare(declarer)
  }
}

object MetricSpout {
  val componentId: String = "MetricSpout"
}
