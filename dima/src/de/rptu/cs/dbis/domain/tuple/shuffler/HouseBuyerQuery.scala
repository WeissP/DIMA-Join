package de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer

import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.domain.tuples.attribute.{Attribute, QueryRelation}

enum HouseBuyerPredicate(
  val houseAttr: Attribute,
  val buyerAttr: Attribute,
  val houseScanOp: ScanOperator,
  val buyerScanOp: ScanOperator,
) {
  case Price
      extends HouseBuyerPredicate(
        House.priceAttr,
        Buyer.maxPriceAttr,
        ScanOperator.Greater,
        ScanOperator.Less,
      )
  case Area
      extends HouseBuyerPredicate(
        House.areaAttr,
        Buyer.minAreaAttr,
        ScanOperator.Less,
        ScanOperator.Greater,
      )
  case Quality1
      extends HouseBuyerPredicate(
        House.quality1Attr,
        Buyer.minQuality1Attr,
        ScanOperator.Greater,
        ScanOperator.Less,
      )
  case Quality2
      extends HouseBuyerPredicate(
        House.quality2Attr,
        Buyer.minQuality2Attr,
        ScanOperator.Greater,
        ScanOperator.Less,
      )
  case Quality3
      extends HouseBuyerPredicate(
        House.quality3Attr,
        Buyer.minQuality3Attr,
        ScanOperator.Greater,
        ScanOperator.Less,
      )

  def value(stream: Stream): Float = {
    this match {
      case Price    => stream.price
      case Area     => stream.area
      case Quality1 => stream.quality1
      case Quality2 => stream.quality2
      case Quality3 => stream.quality3
    }
  }
}

final case class HouseBuyerQuery(
  predicateCount: Int = 5,
  selectedPredicates: List[HouseBuyerPredicate] = Nil,
) {
  require(
    predicateCount >= 1 && predicateCount <= 5,
    s"HouseBuyer predicate count must be between 1 and 5, got $predicateCount",
  )

  require(
    selectedPredicates.isEmpty || selectedPredicates.size == predicateCount,
    "Selected predicates must match the predicate count",
  )
  require(
    selectedPredicates.distinct.size == selectedPredicates.size,
    "Selected predicates must not contain duplicates",
  )

  val predicates: List[HouseBuyerPredicate] = {
    if (selectedPredicates.nonEmpty) { selectedPredicates }
    else { HouseBuyerPredicate.values.take(predicateCount).toList }
  }
  val attributes: List[Attribute] = predicates.map(_.houseAttr) ++
    predicates.map(_.buyerAttr)
  val queryRelation: QueryRelation = predicates
    .flatMap { predicate =>
      List(
        predicate.houseAttr -> predicate.buyerAttr,
        predicate.buyerAttr -> predicate.houseAttr,
      )
    }
    .toMap
}
