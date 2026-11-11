package de.rptu.cs.dbis.domain.tuples.storage

import de.rptu.cs.dbis.domain.TupleId

import cats.syntax.all.*
import de.rptu.cs.dbis.domain.tuples.services.*
import org.apache.storm.tuple.{Fields, Tuple, Values}
import de.rptu.cs.dbis.domain.tuples.dispatcher.StorageRow

// all tuples strictly older than the given timestamp can be safely expired
final case class ExpireTuples(minTuple: TupleId)

object ExpireTuples {
  given TupleMessage[ExpireTuples] with {
    extension (t: ExpireTuples) {
      override def tupleValues = new Values(t.minTuple)
    }
  }

  def fromTuple(tuple: Tuple): ExpireTuples = {
    ExpireTuples(tuple.getValue(0).asInstanceOf[TupleId])
  }
}

case object ExpireTimeKind extends TupleKind {
  val streamId: String = "expire-time"
  val fields           = new Fields("tuple-id")
  val direct           = false
}

case class ResultAttrValues(
  targetValues: Array[Any],
  foundValues: Array[StorageRow],
)

final case class StorageResults(
  storageIdx: Int,
  target: TupleId,
  targetTs: Long,
  outputTime: Long,
  values: ResultAttrValues,
)

object StorageResults {
  given TupleMessage[StorageResults] with {
    extension (t: StorageResults) {
      override def tupleValues =
        new Values(t.storageIdx, t.target, t.targetTs, t.outputTime, t.values)
    }
  }

  def fromTuple(tuple: Tuple): StorageResults = {
    StorageResults(
      tuple.getInteger(0).nn,
      tuple.getValue(1).asInstanceOf[TupleId],
      tuple.getLong(2).nn,
      tuple.getLong(3).nn,
      tuple.getValue(4).asInstanceOf[ResultAttrValues],
    )
  }
}

case object StorageResultsKind extends TupleKind {
  val streamId: String = "storage-results"
  val fields =
    new Fields("storage-idx", "target", "targetTs", "outputTime", "values")
  val direct = false
}
