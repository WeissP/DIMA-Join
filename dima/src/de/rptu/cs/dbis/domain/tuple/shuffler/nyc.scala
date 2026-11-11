package de.rptu.cs.dbis.domain.tuples.shuffler

import cats.Show
import cats.derived.*
import de.rptu.cs.dbis.domain.{Attr, TupleId}
import de.rptu.cs.dbis.domain.tuples.dispatcher.{
  Index,
  Query,
  StorageRow,
  TargetAttrValues,
}
import de.rptu.cs.dbis.domain.tuples.services.*
import org.apache.storm.tuple.{Fields, Tuple, Values}
import de.rptu.cs.dbis.utils.resultsWriter.ResultsWriter
import de.rptu.cs.dbis.domain.tuples.attribute.{Attribute, QueryRelation}
import better.files.File

final case class NycStream(
  tripId: String,
  timestamp: Long,
  fare: Double,
  distance: Double,
  kafkaTime: Long,
  arrivalTime: Long,
) derives Show

object NycStream {
  def resultDir(rootDir: File): File = rootDir / "dima" / "nyc"

  def metricWriter: ResultsWriter = ResultsWriter(extraTargetCols =
    List((3, "kafka-time"), (4, "arrival-time")),
  )

  def query1ResultsWriter: ResultsWriter = ResultsWriter(
    extraTargetCols =
      List((0, "targetTripIdStr"), (1, "targetFare"), (2, "targetDistance")),
    extraFoundValuesCols =
      List((0, "foundTripIdStr"), (1, "foundFare"), (2, "foundDistance")),
  )

  val fields = new Fields(
    "trip-id",
    "timestamp",
    "fare",
    "distance",
    "kafka-time",
    "arrival-time",
  )

  val fareAttribute     = Attribute("fare")
  val distanceAttribute = Attribute("distance")

  given TupleMessage[NycStream] with {
    extension (t: NycStream) {
      override def tupleValues = new Values(
        t.tripId,
        t.timestamp,
        t.fare,
        t.distance,
        t.kafkaTime,
        t.arrivalTime,
      )
    }
  }

  given SpoutTupleStream[NycStream] with {
    val componentIds: List[String] = List("NycFareDistSpout")

    def fromTuple(tuple: Tuple): NycStream = {
      NycStream(
        tuple.getString(0).nn,
        tuple.getLong(1).nn,
        tuple.getDouble(2).nn,
        tuple.getDouble(3).nn,
        tuple.getLong(4).nn,
        tuple.getLong(5).nn,
      )
    }

    val attributes: List[Attribute] = List(fareAttribute, distanceAttribute)
    val queryRelation: QueryRelation = Map(
      fareAttribute     -> fareAttribute,
      distanceAttribute -> distanceAttribute,
    )

    extension (s: NycStream) {
      def clock = s.timestamp

      def storageRow(tid: TupleId): StorageRow = {
        StorageRow(tid.localId, s.timestamp, s.tripId, s.fare, s.distance)
      }

      def targetAttrValues(tid: TupleId): TargetAttrValues = {
        TargetAttrValues(
          tid,
          s.tripId,
          s.fare,
          s.distance,
          s.kafkaTime,
          s.arrivalTime,
        )
      }

      def indexValues(tid: TupleId): List[(String, Values)] = List(
        (
          fareAttribute.indexKind.streamId,
          Index(s.timestamp, Attr(tid, s.fare)).tupleValues,
        ),
        (
          distanceAttribute.indexKind.streamId,
          Index(s.timestamp, Attr(tid, s.distance)).tupleValues,
        ),
      )

      def queryValues(tid: TupleId): List[(String, Values)] = List(
        (
          fareAttribute.queryKind.streamId,
          Query(tid, s.timestamp, s.fare).tupleValues,
        ),
        (
          distanceAttribute.queryKind.streamId,
          Query(tid, s.timestamp, s.distance).tupleValues,
        ),
      )
    }
  }
}
