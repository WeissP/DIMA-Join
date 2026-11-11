package de.rptu.cs.dbis

import de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer.{
  HouseBuyerPredicate,
  HouseBuyerQuery,
}

@main
def submitTopologyHouseBuyerPredicates(args: String*): Unit = {
  val (query, remaining) = HouseBuyerPredicates.parse(args.toList)
  submitHouseBuyerQuery(remaining, Some(query))
}

private[dbis] object HouseBuyerPredicates {
  def parse(args: List[String]): (HouseBuyerQuery, List[String]) = {
    val positions = args
      .zipWithIndex
      .collect { case ("--predicates", index) => index }
    require(
      positions.size == 1,
      "Provide --predicates once, e.g. --predicates price,area",
    )
    val index = positions.head
    val value = args
      .lift(index + 1)
      .getOrElse {
        throw new IllegalArgumentException(
          "--predicates requires a comma-separated list",
        )
      }
    val predicates = value
      .split(",", -1)
      .nn
      .toList
      .map { raw =>
        val name = raw.nn.trim.nn
        HouseBuyerPredicate
          .values
          .find(_.toString.equalsIgnoreCase(name))
          .getOrElse {
            throw new IllegalArgumentException(
              s"Unknown predicate '$name'; expected price, area, quality1, quality2, or quality3",
            )
          }
      }
    val query = HouseBuyerQuery(predicates.size, predicates)
    (query, args.take(index) ++ args.drop(index + 2))
  }
}
