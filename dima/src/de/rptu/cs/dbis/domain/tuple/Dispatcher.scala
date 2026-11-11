package de.rptu.cs.dbis.domain.tuples.dispatcher

import cats.Show
import cats.derived.*
import cats.kernel.{Eq, Order}
import cats.syntax.all.*
import de.rptu.cs.dbis.domain.{Attr, TupleId}
import de.rptu.cs.dbis.domain.tuples.services.*
import org.apache.storm.tuple.{Fields, Tuple, Values}

case class IndexKind(val attrName: String) extends TupleKind {
  val streamId: String = s"index-$attrName"
  val fields           = new Fields("timestamp", "attr")
  val direct           = false
}

final case class Index[T](timestamp: Long, attr: Attr[T]) derives Show

object Index {
  def fromTuple[T](tuple: Tuple): Index[T] = {
    Index(tuple.getLong(0).nn, tuple.getValue(1).asInstanceOf[Attr[T]])
  }

  given [T]: TupleMessage[Index[T]] with {
    extension (t: Index[T]) {
      override def tupleValues = new Values(t.timestamp, t.attr)
    }
  }

  given [T](using indexerAttrName: String): HasTupleKind[Index[T]] with {
    extension (t: Index[T]) { def kind = IndexKind(indexerAttrName) }
  }
}

final case class Query[K](tupleId: TupleId, timestamp: Long, key: K)
    derives Eq, Show

object Query {
  given [K]: TupleMessage[Query[K]] with {
    extension (t: Query[K]) {
      override def tupleValues = new Values(t.tupleId, t.timestamp, t.key)
    }
  }

  given [K: Order]: Order[Query[K]] = Order
    .whenEqualMonoid[Query[K]]
    .combineAll(
      List(Order.by(_.timestamp), Order.by(_.tupleId), Order.by(_.key)),
    )

  given [K: Order]: Ordering[Query[K]] = summon[Order[Query[K]]].toOrdering

  def fromTuple[K](tuple: Tuple): Query[K] = {
    Query(
      tuple.getValue(0).asInstanceOf[TupleId],
      tuple.getLong(1).nn,
      tuple.getValue(2).asInstanceOf[K],
    )
  }
}

case class QueryKind(val attrName: String) extends TupleKind {
  val streamId: String = s"query-$attrName"
  val fields           = new Fields("tuple-id", "timestamp", "interval")
  val direct           = false
}

final case class StorageRow(
  localId: Int,
  timestamp: Long,
  attrValues: Array[Any],
)

object StorageRow {
  given TupleMessage[StorageRow] with {
    extension (t: StorageRow) {
      override def tupleValues =
        new Values(t.localId, t.timestamp, t.attrValues)
    }
  }
  def apply(localId: Int, timestamp: Long, values: Any*): StorageRow =
    new StorageRow(localId, timestamp, values.toArray)

  def fromTuple(tuple: Tuple): StorageRow = {
    StorageRow(
      tuple.getInteger(0).nn,
      tuple.getLong(1).nn,
      tuple.getValue(2).asInstanceOf[Array[Any]],
    )
  }
}

case object StorageRowKind extends TupleKind {
  val streamId = "storage-row"
  val fields   = new Fields("local-id", "timestamp", "values")
  val direct   = true
}

final case class TargetAttrValues(tupleId: TupleId, attrValues: Array[Any])

object TargetAttrValues {
  given TupleMessage[TargetAttrValues] with {
    extension (t: TargetAttrValues) {
      override def tupleValues = new Values(t.tupleId, t.attrValues)
    }
  }
  def apply(tupleId: TupleId, values: Any*): TargetAttrValues =
    new TargetAttrValues(tupleId, values.toArray)

  def fromTuple(tuple: Tuple): TargetAttrValues = {
    TargetAttrValues(
      tuple.getValue(0).asInstanceOf[TupleId],
      tuple.getValue(1).asInstanceOf[Array[Any]],
    )
  }
}

case object TargetAttrValuesKind extends TupleKind {
  val streamId = "target-attr-values"
  val fields   = new Fields("tuple-id", "values")
  val direct   = true
}
