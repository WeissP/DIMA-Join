package de.rptu.cs.dbis.domain.tuples.indexer

import cats.Show
import de.rptu.cs.dbis.domain.{Attr, TupleId}
import de.rptu.cs.dbis.domain.tuples.services.*

import org.apache.storm.tuple.{Fields, Tuple, Values}
import de.rptu.cs.dbis.collection.bitset.{Bitset, Bitsets}
import de.rptu.cs.dbis.collection.initable.Initable
import scala.reflect.ClassTag

case object SendPartitionKind extends TupleKind {
  val streamId: String = "send-partition"
  val fields           = new Fields("values")
  val direct           = true
}

final case class SendPartition[T](values: Array[Attr[T]])

object SendPartition {
  given [T]: TupleMessage[SendPartition[T]] with {
    extension (t: SendPartition[T]) {
      override def tupleValues = new Values(t.values)
    }
  }
  def fromTuple[T](tuple: Tuple): SendPartition[T] = {
    SendPartition(tuple.getValue(0).asInstanceOf[Array[Attr[T]]])
  }
}

final case class QueryPartition[N](tupleId: TupleId, key: N)

object QueryPartition {
  given [N]: Show[QueryPartition[N]] = Show.fromToString

  given [N]: TupleMessage[QueryPartition[N]] with {
    extension (t: QueryPartition[N]) {
      override def tupleValues = new Values(t.tupleId, t.key)
    }
  }

  def fromTuple[N](tuple: Tuple): QueryPartition[N] = {
    QueryPartition(
      tuple.getValue(0).asInstanceOf[TupleId],
      tuple.getValue(1).asInstanceOf[N],
    )
  }
}

case object QueryPartitionKind extends TupleKind {
  val streamId: String = "query-partition"
  val fields           = new Fields("tuple-id", "interval")
  val direct           = true
}

final case class JoinQuery[BS](
  tupleId: TupleId,
  timestamp: Long,
  attrName: String,
  partitionNum: Int,
  tuples: Bitsets[BS],
)

object JoinQuery {
  given [BS](using
    Initable[Unit, BS],
    ClassTag[BS],
    Bitset[BS],
  ): TupleMessage[JoinQuery[BS]] with {
    extension (t: JoinQuery[BS]) {
      override def tupleValues = new Values(
        t.tupleId,
        t.timestamp,
        t.attrName,
        t.partitionNum,
        t.tuples.toBytes,
      )
    }
  }
  def fromTuple[BS](
    tuple: Tuple,
  )(using Initable[Unit, BS], ClassTag[BS], Bitset[BS]): JoinQuery[BS] = {
    JoinQuery(
      tuple.getValue(0).asInstanceOf[TupleId],
      tuple.getLong(1).nn,
      tuple.getString(2).nn,
      tuple.getInteger(3).nn,
      Bitsets.fromBytes[BS](tuple.getValue(4).asInstanceOf[Array[Byte]]),
    )
  }
}

case object JoinQueryKind extends TupleKind {
  val streamId: String = "join-query"
  val fields = new Fields(
    "tuple-id",
    "timestamp",
    "attr-name",
    "partition-num",
    "attributes",
  )
  val direct = true
}
