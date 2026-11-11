package de.rptu.cs.dbis.domain.tuples.shuffler.taq

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

final case class Stream(
  id: Long,
  price: Double,
  qty: Double,
  timestamp: Long,
  arrivalTime: Long,
)

opaque type Trade = Stream

extension (s: Trade) {
  def tradePrice: Double = { s.price }
  def tradeSize: Double  = { s.qty }
}

object Trade {
  val spoutComponentId = "Trade"
  val priceAttr        = Attribute("trade_price")
  val sizeAttr         = Attribute("trade_size")
}

opaque type Quote = Stream

extension (s: Quote) {
  def askPrice: Double = { s.price }
  def askSize: Double  = { s.qty }
}

object Quote {
  val spoutComponentId = "Quote"
  val priceAttr        = Attribute("ask_price")
  val sizeAttr         = Attribute("ask_size")
}

case class TaqStream(isStreamR: Boolean, stream: Stream)

object TaqStream {
  import scala.language.unsafeNulls

  def resultDir(rootDir: File): File = { rootDir / "dima" / "taq" }

  val fields =
    new Fields("isStreamR", "id", "price", "qty", "timestamp", "arrivalTime")

  val benchResultsWriter =
    ResultsWriter(extraTargetCols = List((4, "arrival-time")))
  val debugResultsWriter = ResultsWriter(
    extraTargetCols = List(0 -> "target-id", 4 -> "arrival-time"),
    extraFoundValuesCols = List(3 -> "quote-time"),
  )

  given TupleMessage[TaqStream] with {
    extension (t: TaqStream) {
      override def tupleValues = {
        new Values(
          t.isStreamR,
          t.stream.id,
          t.stream.price,
          t.stream.qty,
          t.stream.timestamp,
          t.stream.arrivalTime,
        )
      }
    }
  }

  given SpoutTupleStream[TaqStream] with {

    override val componentIds: List[String] = List("taq-spout")

    override def fromTuple(tuple: Tuple): TaqStream = {
      TaqStream(
        tuple.getBoolean(0),
        Stream(
          tuple.getLong(1).nn,
          tuple.getDouble(2).nn,
          tuple.getDouble(3).nn,
          tuple.getLong(4).nn,
          tuple.getLong(5).nn,
        ),
      )
    }

    override val attributes: List[Attribute] =
      List(Trade.priceAttr, Trade.sizeAttr, Quote.priceAttr, Quote.sizeAttr)

    val queryRelation: QueryRelation = Map(
      Trade.priceAttr -> Quote.priceAttr,
      Quote.priceAttr -> Trade.priceAttr,
      Trade.sizeAttr  -> Quote.sizeAttr,
      Quote.sizeAttr  -> Trade.sizeAttr,
    )

    extension (s: TaqStream) {
      override def clock: Long = { s.stream.timestamp }

      override def storageRow(tid: TupleId): StorageRow = {
        StorageRow(
          tid.localId,
          s.clock,
          s.stream.id,
          s.stream.price,
          s.stream.qty,
          s.stream.timestamp,
        )
      }

      override def targetAttrValues(tid: TupleId): TargetAttrValues = {
        TargetAttrValues(
          tid,
          s.stream.id,
          s.stream.price,
          s.stream.qty,
          s.stream.timestamp,
          s.stream.arrivalTime,
        )
      }

      override def indexValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            Trade.priceAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.tradePrice)).tupleValues,
            Trade.sizeAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.tradeSize)).tupleValues,
          )
        } else {
          List(
            Quote.priceAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.askPrice)).tupleValues,
            Quote.sizeAttr.indexKind.streamId ->
              Index(s.clock, Attr(tid, s.stream.askSize)).tupleValues,
          )
        }
      }

      override def queryValues(tid: TupleId): List[(String, Values)] = {
        if (s.isStreamR) {
          List(
            Trade.priceAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.tradePrice).tupleValues,
            Trade.sizeAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.tradeSize).tupleValues,
          )
        } else {
          List(
            Quote.priceAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.askPrice).tupleValues,
            Quote.sizeAttr.queryKind.streamId ->
              Query(tid, s.clock, s.stream.askSize).tupleValues,
          )
        }
      }
    }
  }
}
