package de.rptu.cs.dbis.component.dispatcher

import cats.syntax.all.*
import de.rptu.cs.dbis.component.joiner.Joiner
import de.rptu.cs.dbis.component.storage.Storage
import de.rptu.cs.dbis.domain.TupleId
import scala.jdk.CollectionConverters.*
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv.{assertEqIfDbg, ifDbg}
import de.rptu.cs.dbis.domain.tuples.dispatcher.*
import de.rptu.cs.dbis.domain.tuples.shuffler.SpoutTupleStream
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import com.typesafe.scalalogging.StrictLogging
import scala.collection.mutable

class Dispatcher[S](cfg: Config)(using sp: SpoutTupleStream[S])
    extends BaseRichBolt with StrictLogging {
  var dispatcherId: Int                                         = -1
  var dispatcherIdx: Int                                        = -1
  private var localId: Int                                      = 0
  private var collector: OutputCollector                        = _
  private var storageTasks: Array[Integer]                      = _
  private var joinerTasks: Array[Integer]                       = _
  private var incomingMsgs: Map[Int, mutable.Queue[(S, Tuple)]] = _
  private var spoutStreamTaskNum: Int                           = _

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    import scala.language.unsafeNulls
    this.collector = collector
    dispatcherId = context.getThisTaskId()
    dispatcherIdx = context.getThisTaskIndex()
    storageTasks = context
      .getComponentTasks(Storage.componentId)
      .asScala
      .take(cfg.storageNumber)
      .toArray
    assertEqIfDbg(cfg.storageNumber, storageTasks.length)
    joinerTasks = context
      .getComponentTasks(Joiner.componentId)
      .asScala
      .take(cfg.joinerNumber)
      .toArray
    assertEqIfDbg(cfg.joinerNumber, joinerTasks.length)

    spoutStreamTaskNum = sp
      .componentIds
      .map(context.getComponentTasks(_).size)
      .sum
    if (spoutStreamTaskNum > 1) {
      incomingMsgs = Map.empty[Int, mutable.Queue[(S, Tuple)]]
    }
    logger.info(show"Starting topology with config $cfg")
  }

  private def processTuple(tuple: Tuple) = {
    val s       = sp.fromTuple(tuple)
    val tupleId = TupleId(dispatcherIdx, localId)
    localId += 1

    collector.emitDirect(
      storageTasks(cfg.getStorageIdx(tupleId)),
      StorageRowKind.streamId,
      s.storageRow(tupleId).tupleValues,
    )

    collector.emitDirect(
      joinerTasks(cfg.getJoinerIdx(tupleId)),
      TargetAttrValuesKind.streamId,
      tuple,
      s.targetAttrValues(tupleId).tupleValues,
    )

    for ((streamId, values) <- s.indexValues(tupleId)) {
      collector.emit(streamId, values)
    }

    for ((streamId, values) <- s.queryValues(tupleId)) {
      collector.emit(streamId, values)
    }

    collector.ack(tuple)
  }

  override def execute(input: Tuple): Unit = {
    if (spoutStreamTaskNum == 1) { processTuple(input) }
    else {
      val newMsg       = sp.fromTuple(input)
      val sourceTaskId = input.getSourceTask()

      incomingMsgs.get(sourceTaskId) match {
        case None => {
          incomingMsgs = incomingMsgs
            .updated(sourceTaskId, mutable.Queue((newMsg, input)))
        }
        case Some(queue) => { queue.enqueue((newMsg, input)) }
      }

      while (
        incomingMsgs.size == spoutStreamTaskNum &&
        incomingMsgs.values.forall(_.nonEmpty)
      ) {
        val minQueue = incomingMsgs.minBy { case (_, q) => q.head._1.clock }._2
        val (s, tuple) = minQueue.dequeue()
        processTuple(tuple)
      }

      ifDbg {
        if (incomingMsgs.map(_._2.size).sum > spoutStreamTaskNum * 10) {
          logger.warn(
            show"too many staged incoming messages: ${incomingMsgs.view.mapValues(_.toList.map(_._1.clock).show).toMap}",
          )
        }
      }
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    StorageRowKind.declare(declarer)
    TargetAttrValuesKind.declare(declarer)
    sp.declareOutputFields(declarer)
  }
}

object Dispatcher {
  val componentId: String = "Dispatcher"
}
