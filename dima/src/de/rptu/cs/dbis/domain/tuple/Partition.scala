package de.rptu.cs.dbis.domain.tuples.partition

import cats.Show
import cats.derived.*
import de.rptu.cs.dbis.domain.interval.Interval
import de.rptu.cs.dbis.domain.{CollectionStatistic, TupleId}
import de.rptu.cs.dbis.domain.tuples.services.*

import org.apache.storm.tuple.{Fields, Tuple, Values}
import de.rptu.cs.dbis.collection.bitset.{Bitset, Bitsets}
import de.rptu.cs.dbis.collection.initable.Initable
import scala.reflect.ClassTag

final case class PartitionJoinQuery[BS](
  tupleId: TupleId,
  attrName: String,
  tuples: Bitsets[BS],
)

object PartitionJoinQuery {
  def tupleValues[BS](
    tupleId: TupleId,
    attrName: String,
    tupleBytes: Array[Byte],
  ): Values = { new Values(tupleId, attrName, tupleBytes) }

  given [BS](using
    Initable[Unit, BS],
    ClassTag[BS],
    Bitset[BS],
  ): TupleMessage[PartitionJoinQuery[BS]] with {
    extension (t: PartitionJoinQuery[BS]) {
      override def tupleValues =
        new Values(t.tupleId, t.attrName, t.tuples.toBytes)
    }
  }

  def fromTuple[BS](tuple: Tuple)(using
    Initable[Unit, BS],
    ClassTag[BS],
    Bitset[BS],
  ): PartitionJoinQuery[BS] = {
    PartitionJoinQuery(
      tuple.getValue(0).asInstanceOf[TupleId],
      tuple.getString(1).nn,
      Bitsets.fromBytes[BS](tuple.getValue(2).asInstanceOf[Array[Byte]]),
    )
  }
}

case object PartitionJoinQueryKind extends TupleKind {
  val streamId: String = "partition-join-query"
  val fields           = new Fields("tuple-id", "attribute", "tuples")
  val direct           = true
}

final case class PartitionRange(partitionIdx: Int, interval: Option[Interval])

object PartitionRange {
  given TupleMessage[PartitionRange] with {
    extension (t: PartitionRange) {
      override def tupleValues = new Values(t.partitionIdx, t.interval)
    }
  }
  def fromTuple(tuple: Tuple): PartitionRange = {
    PartitionRange(
      tuple.getInteger(0).nn,
      tuple.getValue(1).asInstanceOf[Option[Interval]],
    )
  }
}

case object PartitionRangeKind extends TupleKind {
  val streamId: String = "partition-range"
  val fields           = new Fields("partition-idx", "intervals")
  val direct           = false
}

final case class PartitionMetric(
  attrName: String,
  partitionIdx: Int,
  bucketsInfo: Map[Int, CollectionStatistic],
) derives Show

object PartitionMetric {
  given TupleMessage[PartitionMetric] with {
    extension (t: PartitionMetric) {
      override def tupleValues =
        new Values(t.attrName, t.partitionIdx, t.bucketsInfo)
    }
  }

  def fromTuple(tuple: Tuple): PartitionMetric = {
    PartitionMetric(
      tuple.getString(0).nn,
      tuple.getInteger(1).nn,
      tuple.getValue(2).asInstanceOf[Map[Int, CollectionStatistic]],
    )
  }
}

case object PartitionMetricKind extends TupleKind {
  val streamId: String = "partition-metric"
  val fields = new Fields("attr-name", "partition-idx", "buckets-info")
  val direct = false
}
