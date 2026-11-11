package de.rptu.cs.dbis.domain.tuples.shuffler.spo

import better.files.*
import org.apache.storm.tuple.Tuple

import org.apache.storm.tuple.Values
import de.rptu.cs.dbis.domain.tuples.dispatcher.StorageRow
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.tuples.dispatcher.TargetAttrValues
import de.rptu.cs.dbis.domain.Attr
import de.rptu.cs.dbis.domain.tuples.attribute.Attribute
import de.rptu.cs.dbis.domain.tuples.attribute.QueryRelation
import de.rptu.cs.dbis.domain.tuples.shuffler.SpoutTupleStream
import de.rptu.cs.dbis.domain.tuples.dispatcher.Index
import de.rptu.cs.dbis.domain.tuples.dispatcher.Query
import de.rptu.cs.dbis.domain.tuples.services.TupleMessage
import org.apache.storm.tuple.Fields
import de.rptu.cs.dbis.utils.resultsWriter.ResultsWriter

final case class Stream(id: Int, valueA: Int, valueB: Int, arrivalTime: Long)

opaque type StreamR = Stream

extension (s: StreamR) {
  def duration: Int = s.valueA
  def revenue: Int  = s.valueB
}

object StreamR {
  val spoutComponentId = "StreamR"
  val durationAttr     = Attribute("duration")
  val revenueAttr      = Attribute("revenue")
}

opaque type StreamS = Stream

extension (s: StreamS) {
  def time: Int = s.valueA
  def cost: Int = s.valueB
}

object StreamS {
  val spoutComponentId = "StreamS"
  val timeAttr         = Attribute("time")
  val costAttr         = Attribute("cost")
}

case class SpoStream(isStreamR: Boolean, stream: Stream)

object SpoStream {
  import scala.language.unsafeNulls

  def resultDir(rootDir: File): File = rootDir / "dima" / "spo"

  val fields = new Fields("isStreamR", "id", "valueA", "valueB", "arrivalTime")

  val benchResultsWriter =
    ResultsWriter(extraTargetCols = List((3, "arrival-time")))
  val debugResultsWriter = ResultsWriter(
    extraTargetCols = List(0 -> "target-id", 3 -> "arrival-time"),
    extraFoundValuesCols = List(0 -> "found-id"),
  )

  given TupleMessage[SpoStream] with {
    extension (t: SpoStream) {
      override def tupleValues = new Values(
        t.isStreamR,
        t.stream.id,
        t.stream.valueA,
        t.stream.valueB,
        t.stream.arrivalTime,
      )
    }
  }

  given SpoutTupleStream[SpoStream] with {

    override val componentIds: List[String] = List("spo-spout")

    override def fromTuple(tuple: Tuple): SpoStream = {
      SpoStream(
        tuple.getBoolean(0),
        Stream(
          tuple.getInteger(1),
          tuple.getInteger(2),
          tuple.getInteger(3),
          tuple.getLong(4),
        ),
      )
    }

    override val attributes: List[Attribute] = List(
      StreamR.revenueAttr,
      StreamR.durationAttr,
      StreamS.timeAttr,
      StreamS.costAttr,
    )

    val queryRelation: QueryRelation = Map(
      StreamR.revenueAttr  -> StreamS.costAttr,
      StreamS.costAttr     -> StreamR.revenueAttr,
      StreamR.durationAttr -> StreamS.timeAttr,
      StreamS.timeAttr     -> StreamR.durationAttr,
    )

    extension (s: SpoStream) {
      override def clock: Long = s.stream.id.toLong

      override def storageRow(tid: TupleId): StorageRow = StorageRow(
        tid.localId,
        s.clock,
        s.stream.id,
        s.stream.valueA,
        s.stream.valueB,
      )

      override def targetAttrValues(tid: TupleId): TargetAttrValues = {
        TargetAttrValues(
          tid,
          s.stream.id,
          s.stream.valueA,
          s.stream.valueB,
          s.stream.arrivalTime,
        )
      }

      override def indexValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            StreamR.durationAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.duration)).tupleValues,
            StreamR.revenueAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.revenue)).tupleValues,
          )
        } else {
          List(
            StreamS.timeAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.time)).tupleValues,
            StreamS.costAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.cost)).tupleValues,
          )
        }
      }

      override def queryValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            StreamR.durationAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.duration).tupleValues,
            StreamR.revenueAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.revenue).tupleValues,
          )
        } else {
          List(
            StreamS.timeAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.time).tupleValues,
            StreamS.costAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.cost).tupleValues,
          )
        }
      }
    }
  }
}
