package de.rptu.cs.dbis.domain.tuples.shuffler.blond

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

final case class Stream(id: Int, power: Int, cool: Int, arrivalTime: Long)

object StreamR {
  val spoutComponentId = "StreamR"
  val powerAttr        = Attribute("r_power")
  val coolAttr         = Attribute("r_cool")
}

object StreamS {
  val spoutComponentId = "StreamS"
  val powerAttr        = Attribute("s_power")
  val coolAttr         = Attribute("s_cool")
}

case class BlondStream(isStreamR: Boolean, stream: Stream)

object BlondStream {
  import scala.language.unsafeNulls

  def resultDir(rootDir: File): File = rootDir / "dima" / "blond"

  val fields = new Fields("isStreamR", "id", "power", "cool", "arrivalTime")

  val benchResultsWriter =
    ResultsWriter(extraTargetCols = List((3, "arrival-time")))
  val debugResultsWriter = ResultsWriter(
    extraTargetCols = List(0 -> "target-id", 3 -> "arrival-time"),
    extraFoundValuesCols = List(0 -> "found-id"),
  )

  given TupleMessage[BlondStream] with {
    extension (t: BlondStream) {
      override def tupleValues = new Values(
        t.isStreamR,
        t.stream.id,
        t.stream.power,
        t.stream.cool,
        t.stream.arrivalTime,
      )
    }
  }

  given SpoutTupleStream[BlondStream] with {

    override val componentIds: List[String] = List("blond-spout")

    override def fromTuple(tuple: Tuple): BlondStream = {
      BlondStream(
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
      StreamR.powerAttr,
      StreamR.coolAttr,
      StreamS.powerAttr,
      StreamS.coolAttr,
    )

    val queryRelation: QueryRelation = Map(
      StreamR.powerAttr -> StreamS.powerAttr,
      StreamS.powerAttr -> StreamR.powerAttr,
      StreamR.coolAttr  -> StreamS.coolAttr,
      StreamS.coolAttr  -> StreamR.coolAttr,
    )

    extension (s: BlondStream) {
      override def clock: Long = s.stream.id.toLong

      override def storageRow(tid: TupleId): StorageRow = StorageRow(
        tid.localId,
        s.clock,
        s.stream.id,
        s.stream.power,
        s.stream.cool,
      )

      override def targetAttrValues(tid: TupleId): TargetAttrValues = {
        TargetAttrValues(
          tid,
          s.stream.id,
          s.stream.power,
          s.stream.cool,
          s.stream.arrivalTime,
        )
      }

      override def indexValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            StreamR.powerAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.power)).tupleValues,
            StreamR.coolAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.cool)).tupleValues,
          )
        } else {
          List(
            StreamS.powerAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.power)).tupleValues,
            StreamS.coolAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.cool)).tupleValues,
          )
        }
      }

      override def queryValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            StreamR.powerAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.power).tupleValues,
            StreamR.coolAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.cool).tupleValues,
          )
        } else {
          List(
            StreamS.powerAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.power).tupleValues,
            StreamS.coolAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.cool).tupleValues,
          )
        }
      }
    }
  }
}
