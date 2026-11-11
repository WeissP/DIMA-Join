package de.rptu.cs.dbis.domain.tuples.attribute

import de.rptu.cs.dbis.domain.tuples.dispatcher.IndexKind
import de.rptu.cs.dbis.domain.tuples.dispatcher.QueryKind

case class Attribute(name: String) {
  val indexKind = IndexKind(name)
  val queryKind = QueryKind(name)
}

type QueryRelation = Map[Attribute, Attribute]
