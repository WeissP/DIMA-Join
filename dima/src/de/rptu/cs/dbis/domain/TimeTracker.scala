package de.rptu.cs.dbis.domain.timeTracker

import scala.collection.mutable.ArrayBuffer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Tuple
import de.rptu.cs.dbis.domain.tuples.metric.MetricMessage
import de.rptu.cs.dbis.domain.tuples.metric
import io.circe.Encoder
import io.circe.generic.auto.*
import de.rptu.cs.dbis.domain.tuples.metric.MetricKind

class TimeTracker(trackTimeIntervalMillis: Long) {
  private var curTimeWindow: Long                       = 0
  private var curTimeWindowDurations: ArrayBuffer[Long] = ArrayBuffer()
  val durations: ArrayBuffer[Long]                      = ArrayBuffer()

  def put(dur: Long): Unit = {
    val nowWindow = System.currentTimeMillis() / trackTimeIntervalMillis
    if (curTimeWindow == 0) { curTimeWindow = nowWindow }
    if (nowWindow == curTimeWindow) { curTimeWindowDurations.append(dur) }
    else {
      val avg = {
        if (curTimeWindowDurations.isEmpty) { 0L }
        else { curTimeWindowDurations.sum / curTimeWindowDurations.size }
      }
      durations.append(avg)
      curTimeWindow = nowWindow
      curTimeWindowDurations = ArrayBuffer(dur)
    }
  }
}

case class DurationPayload(componentId: String, taskIdx: Long, duration: Long)
object DurationPayload {
  val streamId = "duration"
  def metricKind(componentId: String): MetricKind = {
    MetricKind(componentId, streamId, "duration")
  }
}

class BoltWithTimeTracker(
  componentId: String,
  b: BaseRichBolt,
  noOtherMetrics: Boolean,
) extends BaseRichBolt with Serializable {
  private var collector: OutputCollector = _
  private var duration: Long             = 0
  private var taskIdx: Long              = _

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    this.collector = collector
    this.taskIdx = context.getThisTaskIndex()
    b.prepare(conf, context, collector)
  }

  override def execute(tuple: Tuple): Unit = {
    if (tuple.getSourceStreamId().nn == metric.MetricTriggerKind.streamId) {
      val trigger = metric.MetricTrigger.fromTuple(tuple)
      val metricMessage =
        MetricMessage(trigger, DurationPayload(componentId, taskIdx, duration))
      collector.emit(DurationPayload.streamId, metricMessage.tupleValues)
      duration = 0
      if (noOtherMetrics) { return }
    }
    val start = System.currentTimeMillis()
    b.execute(tuple)
    val done = System.currentTimeMillis()
    duration += (done - start)
  }

  override def cleanup(): Unit = b.cleanup()

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    DurationPayload.metricKind(componentId).declare(declarer)
    b.declareOutputFields(declarer)
  }
}
