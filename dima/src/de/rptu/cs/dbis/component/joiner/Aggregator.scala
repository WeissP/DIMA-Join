package de.rptu.cs.dbis.component.joiner

import io.circe.syntax.*
import io.circe.{Encoder, Json}
import cats.syntax.all.*
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv.{assertEqIfDbg, assertIfDbg}
import scala.collection.mutable
import org.apache.storm.tuple.Tuple
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.collection.bitset.Bitsets
import scala.reflect.ClassTag
import com.typesafe.scalalogging.StrictLogging

class AttrTuples[BS](val name: String, cfg: Config)(using
  ClassTag[BS],
  Bitset[BS],
  Initable[Unit, BS],
) extends StrictLogging {
  val result                      = new Bitsets[BS](cfg.dispatcherNumber)
  private var partitionsToWait    = none[Int]
  private var processedPartitions = 0

  def insertPartitionResults(bs: Bitsets[BS]): Unit = {
    result.or(bs)
    processedPartitions += 1
  }
  def insertIndexerResults(partitionsToWait: Int, bs: Bitsets[BS]): Unit = {
    result.or(bs)
    assertEqIfDbg(none[Int], this.partitionsToWait)
    this.partitionsToWait = Some(partitionsToWait)
  }
  def isFinished: Boolean = {
    assertIfDbg {
      partitionsToWait match {
        case None    => true
        case Some(w) => processedPartitions <= w
      }
    }
    partitionsToWait.map(_ === processedPartitions).getOrElse(false)
  }

  def status: Json = {
    if (isFinished && result.isEmpty) { "empty" }
    else {
      val target = partitionsToWait match {
        case Some(value) => value.toString()
        case None        => "??"
      }
      s"$processedPartitions/$target"
    }
  }.asJson
}

class Aggregator[BS](val localId: Int, cfg: Config)(using
  ClassTag[BS],
  Bitset[BS],
  Initable[Unit, BS],
) {
  var anchorTuple             = none[Tuple]
  var timestamp: Option[Long] = none[Long]
  private var targetValues    = none[Array[Any]]
  var result                  = none[AggResult[BS]]
  val map                     = mutable.HashMap.empty[String, AttrTuples[BS]]

  def status: Json = {
    Map(
      ("id"         -> localId.asJson),
      ("partition"  -> map.view.mapValues(_.status).toMap.asJson),
      ("indexer"    -> timestamp.isDefined.asJson),
      ("dispatcher" -> targetValues.isDefined.asJson),
      ("finished"   -> tryRun.asJson),
    ).asJson
  }

  def operateAttr(name: String)(op: AttrTuples[BS] => Unit): Unit = {
    op(map.getOrElseUpdate(name, new AttrTuples[BS](name, cfg)))
    val updated = map(name)
    if (updated.isFinished && anchorTuple.isDefined && updated.result.isEmpty) {
      // if one attr finished with no results, early stop
      result = Some(AggResult(
        anchorTuple.get,
        timestamp.get,
        targetValues.get,
        updated.result,
      ))
    }
  }

  def setTimestamp(ts: Long): Unit = { timestamp = Some(ts) }

  def setTargetValues(v: Array[Any]): Unit = { targetValues = Some(v) }

  def setAnchorTuple(t: Tuple) = { anchorTuple = Some(t) }

  def tryRun: Boolean = {
    (result, anchorTuple, timestamp, targetValues) match {
      case (Some(res), _, _, _) => true
      case (None, Some(tup), Some(ts), Some(tv))
          if map.forall(_._2.isFinished) && map.size == cfg.attrQueryNum => {
        var bs: Bitsets[BS] | Null = null
        for (v <- map.values) {
          if (bs == null) { bs = v.result }
          else { bs.nn.and(v.result) }
        }
        result = Some(AggResult(tup, ts, tv, bs.nn))
        true
      }
      case _ => false
    }
  }
}

case class AggResult[BS](
  anchorTuple: Tuple,
  timestamp: Long,
  targetValues: Array[Any],
  foundValues: Bitsets[BS],
)
