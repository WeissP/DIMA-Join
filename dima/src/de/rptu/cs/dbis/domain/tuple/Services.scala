package de.rptu.cs.dbis.domain.tuples.services

import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.{Fields, Values}

trait TupleKind {
  val streamId: String
  val fields: Fields
  val direct: Boolean

  def declare(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream(streamId, direct, fields)
  }
}

trait HasTupleKind[T] {
  extension (t: T) { def kind: TupleKind }
}

trait TupleMessage[T] {
  extension (t: T) { def tupleValues: Values }
}
