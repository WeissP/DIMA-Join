package de.rptu.cs.dbis.component.joiner

import scala.collection.mutable
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertIfDbg
import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.utils.debug.BuildEnv
import cats.syntax.all.*
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertEqIfDbg
import org.apache.storm.task.OutputCollector
import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.initable.Initable
import com.typesafe.scalalogging.StrictLogging
import scala.util.Random
import io.circe.syntax.*
import io.circe.{Encoder, Json}

class AggQueue[BS](
  cfg: Config,
  dispatcherIdx: Int,
  joinerIdx: Int,
  storageTasks: ArraySeq[Integer],
  collector: OutputCollector,
)(using ClassTag[BS], Bitset[BS], Initable[Unit, BS])
    extends StrictLogging {
  private val q           = mutable.ArrayDeque[Aggregator[BS]]()
  private var warn_length = 20

  val storageTaskForEmptyResult = {
    val arr = cfg.sampledStorages.toArray
    val idx = Random.nextInt(arr.length)
    storageTasks(arr(idx))
  }

  var finishedMaxLid: Int = -1
  def length: Int         = q.length
  def isEmpty: Boolean    = q.isEmpty

  def status: Json = { q.map(_.status).asJson }

  protected def validate() = {
    if (q.size > 1) {
      val step = cfg.joinerPerDispatcher
      q.sliding(2)
        .foreach {
          case mutable.Queue(prev, next) => assert(
              next.localId == prev.localId + step,
              s"Aggregators in queue must be spaced by $step, but found ${prev
                  .localId} and ${next.localId}",
            )
          case _ => ()
        }
    }
  }

  private def getAgg(tupleId: TupleId): Aggregator[BS] = {
    BuildEnv.ifDbg(validate())
    if (q.isEmpty) {
      val newAgg = new Aggregator(tupleId.localId, cfg)
      q.addOne(newAgg)
      newAgg
    } else {
      val step = cfg.joinerPerDispatcher

      val base = {
        var firstLocalId = q.head.localId
        while (firstLocalId > tupleId.localId) {
          firstLocalId -= step
          q.prepend(new Aggregator(firstLocalId, cfg))
        }
        q.head.localId
      }

      {
        var lastLocalId = base + (q.size - 1) * step
        while (lastLocalId < tupleId.localId) {
          lastLocalId += step
          q.append(new Aggregator(lastLocalId, cfg))
        }
      }

      if (q.size > warn_length) {
        logger.warn(s"the queue of size ${q.size} is too long")
        warn_length = q.size
      }

      assertIfDbg(
        show"[Joiner $joinerIdx] target tuple with local Id ${tupleId
            .localId} is less than base $base. Local IDs: ${q.map(_.localId).toList}",
        tupleId.localId >= base,
      )

      val position = (tupleId.localId - base) / step
      assertIfDbg(
        s"[Joiner $joinerIdx] LocalId ${tupleId.localId} not aligned with step $step from base $base",
        (tupleId.localId - base) % step == 0,
      )
      assertIfDbg(
        s"[Joiner $joinerIdx] Position $position out of bounds for size ${q.size}",
        position >= 0 && position < q.size,
      )
      val agg = q(position)
      assertEqIfDbg(
        s"[Joiner $joinerIdx] Aggregator localId mismatch",
        agg.localId,
        tupleId.localId,
      )
      agg
    }
  }

  def updateAgg(tupleId: TupleId, update: Aggregator[BS] => Unit) = {
    if (tupleId.localId > finishedMaxLid) {
      val agg = getAgg(tupleId)
      if (agg.result.isEmpty) { update(agg) }
    }
  }

  def tryProcess: Unit = {
    while (q.headOption.map(_.tryRun).getOrElse(false)) {
      val agg = q.removeHead(false)
      finishedMaxLid = agg.localId
      val aggRes = agg.result.get
      logger.debug( 
        show"[Joiner $joinerIdx] processing target tuple ${TupleId(dispatcherIdx, agg.localId)}",
      )
      if (aggRes.foundValues.isEmpty) {
        val resultsMsg = joiner.Results(
          joinerIdx,
          TupleId(dispatcherIdx, agg.localId),
          agg.timestamp.get,
          aggRes.targetValues,
          ().init,
        )
        collector.emitDirect(
          storageTaskForEmptyResult,
          joiner.ResultsKind.streamId,
          aggRes.anchorTuple,
          resultsMsg.tupleValues,
        )
      } else {
        for (
          (ps, sid) <-
            aggRes.foundValues.storagePostions(cfg).elems.view.zipWithIndex
        ) {
          val resultsMsg = joiner.Results(
            joinerIdx,
            TupleId(dispatcherIdx, agg.localId),
            agg.timestamp.get,
            aggRes.targetValues,
            ps,
          )
          logger.debug(
            show"[Joiner $joinerIdx] Regarding target tuple ${TupleId(dispatcherIdx, agg.localId)}, trying to send local IDs to $sid-th storage",
          )

          collector.emitDirect(
            storageTasks(sid),
            joiner.ResultsKind.streamId,
            aggRes.anchorTuple,
            resultsMsg.tupleValues,
          )
          logger.trace(
            s"[Joiner $joinerIdx] sent join results of target ${agg.localId}",
          )
        }
      }
      logger.trace(
        show"[Joiner $joinerIdx] dequeuing Aggregator for Local ID: ${agg
            .localId} from queue for dispatcher ${dispatcherIdx}. Remaining IDs: ${q.map(_.localId).toList}",
      )
      collector.ack(aggRes.anchorTuple)
    }
  }
}
