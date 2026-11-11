package de.rptu.cs.dbis.domain.tuples.joiner

import cats.Show
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.tuples.services.*
import org.apache.storm.tuple.{Fields, Tuple, Values}
import de.rptu.cs.dbis.collection.bitset.Bitset

final case class Results[BS](
  joinerIdx: Int,
  targetTupleId: TupleId,
  timestamp: Long,
  targetAttrValues: Array[Any],
  positions: BS,
)

object Results {

  given [BS]: Show[Results[BS]] with {
    def show(r: Results[BS]): String = {
      s"Results(joinerIdx=${r.joinerIdx}, target=${r.targetTupleId}, ts=${r
          .timestamp}, target attributes=${r.targetAttrValues.toString}, found=${r.positions})"
    }
  }

  given [BS](using Bitset[BS]): TupleMessage[Results[BS]] with {
    extension (t: Results[BS]) {
      override def tupleValues = new Values(
        t.joinerIdx,
        t.targetTupleId,
        t.timestamp,
        t.targetAttrValues,
        t.positions.toBytes,
      )
    }
  }

  def fromTuple[BS](tuple: Tuple)(using bitset: Bitset[BS]): Results[BS] = {
    Results(
      tuple.getValue(0).asInstanceOf[Int],
      tuple.getValue(1).asInstanceOf[TupleId],
      tuple.getValue(2).asInstanceOf[Long],
      tuple.getValue(3).asInstanceOf[Array[Any]],
      bitset.fromBytes(tuple.getValue(4).asInstanceOf[Array[Byte]]),
    )
  }
}

case object ResultsKind extends TupleKind {
  val streamId: String = "join-results"
  val fields = new Fields(
    "joinerIdx",
    "target",
    "timestamp",
    "target-attributes",
    "tuples",
  )
  val direct = true
}
