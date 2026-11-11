package de.rptu.cs.dbis.domain.tuples.shuffler

import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.tuples.attribute.{Attribute, QueryRelation}
import de.rptu.cs.dbis.domain.tuples.dispatcher.{StorageRow, TargetAttrValues}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.{Tuple, Values}

trait SpoutStream[S] {
  val componentIds: List[String]
  def fromTuple(tuple: Tuple): S
  def declareOutputFields(declarer: OutputFieldsDeclarer): Unit

  extension (s: S) {
    def clock: Long
    def storageRow(tid: TupleId): StorageRow
    def targetAttrValues(tid: TupleId): TargetAttrValues
    def indexValues(tid: TupleId): List[(String, Values)]
    def queryValues(tid: TupleId): List[(String, Values)]
  }
}

trait SpoutTupleStream[S] {
  val componentIds: List[String]
  def fromTuple(tuple: Tuple): S
  val attributes: List[Attribute]
  val queryRelation: QueryRelation
  def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    attributes.foreach(attr => {
      attr.indexKind.declare(declarer)
      attr.queryKind.declare(declarer)
    })
  }

  extension (s: S) {
    def clock: Long
    def storageRow(tid: TupleId): StorageRow
    def targetAttrValues(tid: TupleId): TargetAttrValues
    def indexValues(tid: TupleId): List[(String, Values)]
    def queryValues(tid: TupleId): List[(String, Values)]
  }
}
