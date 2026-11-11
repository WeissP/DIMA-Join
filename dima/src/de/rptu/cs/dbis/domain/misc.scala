package de.rptu.cs.dbis.domain

import cats.Show
import cats.derived.*
import cats.kernel.{Eq, Order}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import io.circe.*
import io.circe.generic.semiauto.*
import scala.collection.mutable
import de.rptu.cs.dbis.services.DoubleOrdering

final case class TupleId(dispatcherIdx: Int, localId: Int)
    derives Eq, Show, Order

trait HasTupleId[O] extends Serializable {
  extension (o: O) {
    def tupleId: TupleId
    def dispatcherIdx: Int = tupleId.dispatcherIdx
    def localId: Int       = tupleId.localId
  }
}

object TupleId {
  given Ordering[TupleId] = Order[TupleId].toOrdering
  given HasTupleId[TupleId] with {
    extension (o: TupleId) override def tupleId: TupleId = o
  }

  val serializer: Serializer[TupleId] = new Serializer[TupleId] {
    override def write(kryo: Kryo, output: Output, tupleId: TupleId): Unit = {
      output.writeInt(tupleId.dispatcherIdx)
      output.writeInt(tupleId.localId)
    }

    override def read(
      kryo: Kryo,
      input: Input,
      clazz: Class[TupleId],
    ): TupleId = {
      val dispatcherIdx = input.readInt()
      val localId       = input.readInt()
      TupleId(dispatcherIdx, localId)
    }
  }
}

enum ScanOperator {
  case Greater, Less
}

trait MapElement[P, K, V] {
  extension (p: P) {
    def elemKey: K
    def elemVal: V
    def updateKey(f: K => K): P
    def toTuple: (K, V) = (elemKey, elemVal)
  }
}

object MapElement {
  given [K, V]: MapElement[(K, V), K, V] with {
    extension (p: (K, V)) {
      def elemKey: K                   = p._1
      def elemVal: V                   = p._2
      def updateKey(f: K => K): (K, V) = (f(p._1), p._2)
    }
  }
}

final case class Attr[T](tupleId: TupleId, value: T) derives Eq, Show

object Attr {
  given partitionMapElement[T]: MapElement[Attr[T], T, TupleId] with {
    extension (p: Attr[T]) {
      def elemKey                       = p.value
      def elemVal                       = p.tupleId
      def updateKey(f: T => T): Attr[T] = p.copy(value = f(p.value))
    }
  }

  given [T: Order]: Order[Attr[T]] = Order
    .whenEqual(Order.by(_.value), Order.by(_.tupleId))
  given [T: Order]: Ordering[Attr[T]] = Order[Attr[T]].toOrdering

  given [T](using num: Numeric[T]): DoubleOrdering[Attr[T]] with {
    extension (v: Attr[T]) {
      def compareToDouble(d: Double): Int = { num.toDouble(v.value).compare(d) }
    }
  }
}

final case class Record(tupleId: TupleId, timestamp: Long, values: Vector[Any])

final class TimestampRecords(
  private var records: mutable.ArraySeq[Long],
  private var minTime: Long,
) extends Serializable {
  def currentMin: Long = { minTime }
  // update timestamp of the given array index, returns true if the minimal timestamp is updated.
  def put(id: Int, time: Long): Boolean = {
    val updated = currentMin == records(id) && time > currentMin
    records(id) = time
    if (updated) { minTime = records.min() }
    updated
  }
}

object TimestampRecords {
  given Show[TimestampRecords] = Show.show { tr =>
    val recordsStr = tr
      .records
      .iterator
      .zipWithIndex
      .map { case (time, idx) => s"$idx -> $time" }
      .mkString("{", ", ", "}")
    s"TimestampRecords(records = $recordsStr, currentMin = ${tr.minTime})"
  }

  def ofCap(cap: Int): TimestampRecords = {
    new TimestampRecords(mutable.ArraySeq.fill(cap)(0), 0)
  }
}

case class CollectionStatistic(
  min: Option[Double],
  max: Option[Double],
  size: Int,
) derives Show

object CollectionStatistic {
  given Decoder[CollectionStatistic] = deriveDecoder[CollectionStatistic]
  given Encoder[CollectionStatistic] = deriveEncoder[CollectionStatistic]

  def apply[T](
    coll: collection.Seq[T],
  )(using num: Numeric[T]): CollectionStatistic = {
    val values = coll.map(num.toDouble)
    CollectionStatistic(values.minOption, values.maxOption, coll.size)
  }
}
