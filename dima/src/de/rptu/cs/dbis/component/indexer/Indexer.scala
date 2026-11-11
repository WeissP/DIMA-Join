package de.rptu.cs.dbis.component.indexer

import cats.Show
import de.rptu.cs.dbis.domain.tuples.dispatcher.Query
import cats.kernel.Order
import cats.syntax.all.*
import com.bigml.histogram.{Histogram, SimpleTarget}
import de.rptu.cs.dbis.component.joiner.Joiner
import de.rptu.cs.dbis.component.partition.Partition
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.utils.debug.BuildEnv
import de.rptu.cs.dbis.utils.debug.BuildEnv.{assertEqIfDbg, assertIfDbg}
import de.rptu.cs.dbis.domain.interval.Interval
import de.rptu.cs.dbis.domain.{Attr, TimestampRecords}
import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.tuples.indexer.SendPartition
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import com.typesafe.scalalogging.StrictLogging
import scala.collection.immutable.ArraySeq
import scala.collection.{View, mutable}
import scala.jdk.CollectionConverters.*
import scala.collection.mutable.ArrayBuffer
import de.rptu.cs.dbis.collection.bitset.Bitset
import scala.reflect.ClassTag
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.collection.bitset.Bitsets
import de.rptu.cs.dbis.collection.sortedBuffer.*
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.domain.interval.Bound
import de.rptu.cs.dbis.domain.TupleId
import org.roaringbitmap.RoaringBitmapWriter

class Indexer[T, BS](
  cfg: Config,
  attrName: String,
  scanOp: ScanOperator,
  initHistogram: => Histogram[SimpleTarget] = Histogram(256, true),
)(using numeric: Numeric[T])(using
  Order[T],
  Show[T],
  ClassTag[BS],
  Bitset[BS],
  Initable[Unit, BS],
) extends BaseRichBolt with StrictLogging {
  private var collector: OutputCollector = _
// If the values in the buffer have not yet been successfully transferred to the partitions, we still need to query the values in this buffer, while new values should be inserted into a new buffer. Thus we set up two buffers and alternately switch between them.
  private var bufferA: ArrayBuffer[Attr[T]]            = _
  private var bufferB: ArrayBuffer[Attr[T]]            = _
  private var primaryBufferIsA                         = true
  private var bufferHistogram: Histogram[SimpleTarget] = _
  private val distributions                            = new Distributions(cfg)
  // the stored values intervals of each partition
  private var partitionIntervals: Array[Option[Interval]] = _
  private var transferIntervals: Array[Option[Interval]]  = _
  // to get the minimum timestamp of indexed tuples, all queries with less timestamp is safe to process
  private var timestampRecords: TimestampRecords = _
  private var queries: Queries[T]                = _
  private var partitionTasks: ArraySeq[Integer]  = _
  private var joinerTasks: ArraySeq[Integer]     = _
  private val triggerStatus                      = TriggerStatus()
  private var isFirstBuffer: Boolean             = true
  private var queryValues: ArrayBuffer[T]        = _
  private var minLocalId: Int                    = 0
  private var maxLocalId: Int                    = -1

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    logger.info(show"[Indexer $attrName] Received config: $cfg")
    this.collector = collector
    bufferA = new ArrayBuffer[Attr[T]](cfg.initialBufferCap)
    bufferB = new ArrayBuffer[Attr[T]](cfg.initialBufferCap)
    bufferHistogram = initHistogram
    partitionIntervals = Array.fill(cfg.partitionNum)(none)
    transferIntervals = Array.fill(cfg.partitionNum)(none)
    partitionTasks = ArraySeq.from(
      context
        .getComponentTasks(Partition.componentId(attrName))
        .nn
        .asScala
        .take(cfg.partitionNum),
    )
    assertEqIfDbg(cfg.partitionNum, partitionTasks.length)
    joinerTasks = ArraySeq.from(
      context
        .getComponentTasks(Joiner.componentId)
        .nn
        .asScala
        .take(cfg.joinerNumber),
    )
    assertEqIfDbg(cfg.joinerNumber, joinerTasks.length)
    if (cfg.dispatcherNumber > 1) {
      queries = Queries.ofCap[T](cfg.dispatcherNumber)
      timestampRecords = TimestampRecords.ofCap(cfg.dispatcherNumber)
    }
    if (cfg.partitionNum > 1) { queryValues = ArrayBuffer() }
  }

  override def execute(tuple: Tuple): Unit = {
    tuple.kind match {
      case TupleKind.Query          => handleQuery(tuple)
      case TupleKind.Index          => handleIndex(tuple)
      case TupleKind.SendPartition  => handleSendPartition(tuple)
      case TupleKind.PartitionRange => handlePartitionRange(tuple)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    List(
      indexer.SendPartitionKind,
      indexer.JoinQueryKind,
      indexer.QueryPartitionKind,
    ).foreach(_.declare(declarer))
  }

  private def handleQuery(t: Tuple): Unit = {
    if (cfg.dispatcherNumber == 1) { sendQuery(t, Query.fromTuple[T](t)) }
    else {
      queries.put(t)
      processQueries
    }
  }

  private def processQueries = {
    queries.processUntil(timestampRecords.currentMin, sendQuery)
  }

  private def sendQuery(t: Tuple, q: dispatcher.Query[T]) = {
    val cands = (0 until cfg.partitionNum).filter(i => {
      partitionIntervals(i).map(_.scanIncluded(q.key, scanOp)).getOrElse(false)
    })

    cands.foreach { i =>
      collector.emitDirect(
        partitionTasks(i),
        indexer.QueryPartitionKind.streamId,
        indexer.QueryPartition(q.tupleId, q.key).tupleValues,
      )
    }

    val foundTuples = Bitsets.fromIter[BS](
      cfg.dispatcherNumber, {
        import scala.language.unsafeNulls
        RoaringBitmapWriter
          .bufferWriter()
          .expectedRange(minLocalId, maxLocalId)
          .expectedValuesPerContainer(math.max(
            (bufferA.size + bufferB.size) /
              (((maxLocalId - minLocalId) >>> 16).toInt + 1),
            65000,
          ))
          .doPartialRadixSort()
          .optimiseForRuns()
          .get()
      },
      scanByKey(q.key)
        .map(attr => (attr.tupleId.dispatcherIdx, attr.tupleId.localId)),
    )

    logger.trace(
      show"[Indexer $attrName] Regarding target tuple ${q.tupleId}, found following IDs: ${foundTuples}",
    )

    collector.emitDirect(
      joinerTasks(cfg.getJoinerIdx(q.tupleId)),
      indexer.JoinQueryKind.streamId,
      indexer
        .JoinQuery(q.tupleId, q.timestamp, attrName, cands.length, foundTuples)
        .tupleValues,
    )

    if (cfg.partitionNum > 1) {
      if (partitionIntervals.forall(_ == None)) { queryValues.addOne(q.key) }
      else {
        if (
          partitionIntervals
            .head
            .flatMap(_.lower)
            .map(_.isAfterBound(q.key)(true))
            .getOrElse(false) &&
          partitionIntervals
            .last
            .flatMap(_.upper)
            .map(_.isBeforeBound(q.key)(true))
            .getOrElse(false)
        ) { bufferHistogram.insert(numeric.toDouble(q.key)) }
      }
    }

    collector.ack(t)
  }

  private def handleIndex(t: Tuple): Unit = {
    val msg = dispatcher.Index.fromTuple[T](t)
    maxLocalId = msg.attr.tupleId.localId
    if (cfg.dispatcherNumber > 1) {
      timestampRecords.put(msg.attr.tupleId.dispatcherIdx, msg.timestamp)
      processQueries
    }

    val buf = primaryBuffer
    buf.sortedInsert(msg.attr)

    if (isBufferFull) {
      if (triggerStatus.isTransfering) {
        logger.warn(
          "the next buffer is full but the previous buffer is not yet finished transfering",
        )
      } else {
        if (isFirstBuffer) { isFirstBuffer = false }
        assertIfDbg(
          show"[Indexer $attrName] backup buffer should be empty",
          backupBuffer.isEmpty,
        )
        val toTransfer = {
          if (cfg.partitionNum > 1) {
            if (queryValues.size > 0) {
              val min = buf.head.value
              val max = buf.last.value
              for (v <- queryValues) {
                if (min <= v && v <= max) {
                  bufferHistogram.insert(numeric.toDouble(v))
                }
              }
              if (queryValues.size > cfg.bufferCap) { queryValues.clear() }
            }
            val splitPoints = distributions.put(bufferHistogram)
            val lastIdx = splitPoints.size
            var bufIdx  = 0
            (0 to lastIdx).map(pidx => {
              if (pidx < lastIdx) {
                val res = ArrayBuffer[Attr[T]]()
                res.sizeHint(cfg.bufferCap)
                val endPoint: Double = splitPoints.get(pidx).nn
                var cont             = true
                while (bufIdx < buf.length - 1 && cont) {
                  val v = buf(bufIdx)
                  cont = v.compareToDouble(endPoint) < 1
                  if (cont) {
                    res.addOne(v)
                    bufIdx += 1
                  }
                }
                res.toArray
              } else { buf.view.slice(bufIdx, buf.length).toArray }
            })
          } else { IndexedSeq(buf.toArray) }
        }
        assertEqIfDbg(
          buf.map(_.tupleId.localId).toList,
          toTransfer.flatten.map(_.tupleId.localId).toList,
        )
        for (i <- toTransfer.indices) {
          val b = toTransfer(i)
          transferIntervals(i) = Option.unless(b.isEmpty)(
            Interval()
              .withLower(Bound.incl(numeric.toDouble(b.head.value)))
              .withUpper(Bound.incl(numeric.toDouble(b.last.value))),
          )
        }

        primaryBufferIsA = !primaryBufferIsA
        primaryBuffer.sizeHint(backupBuffer.size)
        minLocalId = maxLocalId

        if (cfg.partitionNum > 1) { bufferHistogram = initHistogram }

        triggerStatus.setTask { t =>
          logger.debug(s"sending partitions")
          for (idx <- toTransfer.indices) {
            val taskId = partitionTasks(idx)
            collector.emitDirect(
              taskId,
              indexer.SendPartitionKind.streamId,
              t,
              indexer.SendPartition(toTransfer(idx)).tupleValues,
            )
          }

          collector.ack(t)
        }
      }
    }
  }

  private def handlePartitionRange(t: Tuple): Unit = {
    val msg = partition.PartitionRange.fromTuple(t)
    partitionIntervals.update(msg.partitionIdx, msg.interval)
  }

  private def scanByKey(k: T): View[Attr[T]] = {
    val orderByT: Ordering[Attr[T]] = Ordering.by(_.value)

    val mockAttr = Attr(TupleId(0, 0), k)
    if (triggerStatus.isTransfering) {
      bufferA.scanValue(mockAttr, scanOp)(using orderByT).view ++
        bufferB.scanValue(mockAttr, scanOp)(using orderByT).view
    } else { primaryBuffer.scanValue(mockAttr, scanOp)(using orderByT) }
  }

  private def handleSendPartition(t: Tuple): Unit = {
    import trigger.PartitionAction.*
    val msg = trigger.Partition.fromTuple(t)
    logger.debug(s"got trigger $msg")
    msg.action match {
      case Send => triggerStatus.setTrigger(t)
      case Release => {
        for (i <- partitionIntervals.indices) {
          partitionIntervals(i) = {
            (partitionIntervals(i), transferIntervals(i)) match {
              case (None, None)         => None
              case (Some(pi), None)     => Some(pi)
              case (None, Some(ti))     => Some(ti)
              case (Some(pi), Some(ti)) => Some(pi.union(ti))
            }
          }
        }
        backupBuffer.clear()
        triggerStatus.transferSucceed
        collector.ack(t)
      }
    }

  }

  def isBufferFull: Boolean = {
    val cap = {
      if (isFirstBuffer) { cfg.initialBufferCap }
      else cfg.bufferCap
    }
    val len = primaryBuffer.length
    if (len > cap + cap / 2 + 100) {
      logger.warn(
        show"[Indexer $attrName] Too many elements in buffer $len regarding cap $cap.",
      )
    }

    len >= cap
  }

  def primaryBuffer: ArrayBuffer[Attr[T]] = {
    if (primaryBufferIsA) { bufferA }
    else { bufferB }
  }

  def backupBuffer: ArrayBuffer[Attr[T]] = {
    if (primaryBufferIsA) { bufferB }
    else { bufferA }
  }
}

object Indexer {
  def componentId(attrName: String): String = s"Indexer-$attrName"
}

private enum TupleKind {
  case SendPartition
  case Index
  case Query
  case PartitionRange
}

extension (t: Tuple) {
  private def kind: TupleKind = {
    val partitionRange = partition.PartitionRangeKind.streamId
    t.getSourceStreamId().nn match {
      case `partitionRange`               => TupleKind.PartitionRange
      case s if s.startsWith("partition") => TupleKind.SendPartition
      case s if s.startsWith("index")     => TupleKind.Index
      case s if s.startsWith("query")     => TupleKind.Query
      case s: Any => throw new RuntimeException(s"unknown tuple source: $s")
    }
  }
}
