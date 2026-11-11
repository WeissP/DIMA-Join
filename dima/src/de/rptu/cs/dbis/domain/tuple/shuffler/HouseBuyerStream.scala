package de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer

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
import de.rptu.cs.dbis.utils.resultsWriter.{ResultIdsCols, ResultsWriter}

final case class Stream(
  id: Long,
  price: Float,
  area: Float,
  quality1: Float,
  quality2: Float,
  quality3: Float,
  arrivalTime: Long,
)

object House {
  val spoutComponentId = "House"
  val priceAttr        = Attribute("house_price")
  val areaAttr         = Attribute("house_area")
  val quality1Attr     = Attribute("house_quality1")
  val quality2Attr     = Attribute("house_quality2")
  val quality3Attr     = Attribute("house_quality3")
}

object Buyer {
  val spoutComponentId = "Buyer"
  val maxPriceAttr     = Attribute("buyer_max_price")
  val minAreaAttr      = Attribute("buyer_min_area")
  val minQuality1Attr  = Attribute("buyer_min_quality1")
  val minQuality2Attr  = Attribute("buyer_min_quality2")
  val minQuality3Attr  = Attribute("buyer_min_quality3")
}

case class HouseBuyerStream(isStreamR: Boolean, stream: Stream)

object HouseBuyerStream {
  import scala.language.unsafeNulls

  def resultDir(rootDir: File): File = { rootDir / "dima" / "housebuyer" }

  val fields = new Fields(
    "isStreamR",
    "id",
    "price",
    "area",
    "quality1",
    "quality2",
    "quality3",
    "arrivalTime",
  )

  val benchResultsWriter =
    ResultsWriter(extraTargetCols = List((6, "arrival-time")))
  val resultIdsWriter = benchResultsWriter
    .copy(resultIdsCols = Some(ResultIdsCols(targetIdx = 0, foundIdx = 0)))
  val debugResultsWriter = ResultsWriter(
    extraTargetCols = List(0 -> "target-id", 6 -> "arrival-time"),
    extraFoundValuesCols = List(0 -> "found-id"),
  )

  given TupleMessage[HouseBuyerStream] with {
    extension (t: HouseBuyerStream) {
      override def tupleValues = {
        new Values(
          t.isStreamR,
          t.stream.id,
          t.stream.price,
          t.stream.area,
          t.stream.quality1,
          t.stream.quality2,
          t.stream.quality3,
          t.stream.arrivalTime,
        )
      }
    }
  }

  given SpoutTupleStream[HouseBuyerStream] = forQuery(HouseBuyerQuery())

  def forQuery(query: HouseBuyerQuery): SpoutTupleStream[HouseBuyerStream] = {
    new ConfiguredStream(query)
  }

  private class ConfiguredStream(query: HouseBuyerQuery)
      extends SpoutTupleStream[HouseBuyerStream] with Serializable {

    override val componentIds: List[String] = List("housebuyer-spout")

    override def fromTuple(tuple: Tuple): HouseBuyerStream = {
      HouseBuyerStream(
        tuple.getBoolean(0),
        Stream(
          tuple.getLong(1).nn,
          tuple.getFloat(2).nn,
          tuple.getFloat(3).nn,
          tuple.getFloat(4).nn,
          tuple.getFloat(5).nn,
          tuple.getFloat(6).nn,
          tuple.getLong(7).nn,
        ),
      )
    }

    override val attributes: List[Attribute]  = query.attributes
    override val queryRelation: QueryRelation = query.queryRelation

    extension (s: HouseBuyerStream) {
      override def clock: Long = { s.stream.id }

      override def storageRow(tid: TupleId): StorageRow = {
        StorageRow(
          tid.localId,
          s.clock,
          s.stream.id,
          s.stream.price,
          s.stream.area,
          s.stream.quality1,
          s.stream.quality2,
          s.stream.quality3,
        )
      }

      override def targetAttrValues(tid: TupleId): TargetAttrValues = {
        TargetAttrValues(
          tid,
          s.stream.id,
          s.stream.price,
          s.stream.area,
          s.stream.quality1,
          s.stream.quality2,
          s.stream.quality3,
          s.stream.arrivalTime,
        )
      }

      override def indexValues(tid: TupleId): List[(String, Values)] = {
        query
          .predicates
          .map { predicate =>
            val attr = {
              if (s.isStreamR) { predicate.houseAttr }
              else { predicate.buyerAttr }
            }
            attr.indexKind.streamId ->
              Index(s.clock, Attr(tid, predicate.value(s.stream))).tupleValues
          }
      }

      override def queryValues(tid: TupleId): List[(String, Values)] = {
        query
          .predicates
          .map { predicate =>
            val attr = {
              if (s.isStreamR) { predicate.houseAttr }
              else { predicate.buyerAttr }
            }
            attr.queryKind.streamId ->
              Query(tid, s.clock, predicate.value(s.stream)).tupleValues
          }
      }
    }
  }
}
