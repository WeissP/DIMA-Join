package de.rptu.cs.dbis.utils.generator

import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.interval.Bound
import de.rptu.cs.dbis.domain.interval.Interval
import org.scalacheck.Arbitrary
import de.rptu.cs.dbis.domain.Attr

object Constants {
  val maxInt        = 1000
  val maxDouble     = 10000.0
  val dispatcherNum = 2
  val maxLocalId    = 100000
}

val int: Gen[Int]    = Gen.choose(-Constants.maxInt, Constants.maxInt)
val posInt: Gen[Int] = Gen.choose(0, Constants.maxInt)

val double: Gen[Double] = Gen.choose(-Constants.maxDouble, Constants.maxDouble)

val intSet: Gen[Set[Int]]       = Gen.nonEmptyListOf(int).map(_.toSet)
val posIntSet: Gen[Set[Int]]    = Gen.nonEmptyListOf(posInt).map(_.toSet)
val doubleSet: Gen[Set[Double]] = Gen.nonEmptyListOf(double).map(_.toSet)

val doubleIntSet: Gen[(Set[Int], Set[Int])] = for {
  common  <- Gen.listOf(int)
  values1 <- Gen.nonEmptyListOf(int)
  values2 <- Gen.nonEmptyListOf(int)
} yield (common.appendedAll(values1).toSet, common.appendedAll(values2).toSet)

val tupleId: Gen[TupleId] = for {
  did <- Gen.choose(0, Constants.dispatcherNum - 1)
  lid <- Gen.choose(0, Constants.maxLocalId)
} yield TupleId(did, lid)
given Arbitrary[TupleId] = Arbitrary(tupleId)

val bound: Gen[Bound] = for {
  value <- Gen.choose(-100.0, 100.0)
  incl  <- Gen.oneOf(true, false)
} yield Bound(value, incl)

val interval: Gen[Interval] = Gen.frequency(
  // Generate valid intervals where both bounds are present (70% chance)
  (
    70,
    (for {
      lValue <- Gen.choose(-100.0, 100.0)
      uValue <- Gen.choose(lValue, 100.0) // Ensure upper >= lower
      lIncl  <- arbitrary[Boolean]
      uIncl  <- if (lValue == uValue) Gen.const(true) else arbitrary[Boolean]
    } yield Interval(Some(Bound(lValue, lIncl)), Some(Bound(uValue, uIncl)))),
  ),

  // Generate intervals with only lower bound (10% chance)
  (10, Gen.option(bound).map(l => Interval(l, None))),

  // Generate intervals with only upper bound (10% chance)
  (10, Gen.option(bound).map(u => Interval(None, u))),

  // Empty interval (10% chance)
  (10, Gen.const(Interval(None, None))),
)

def attr[T](using arb: Arbitrary[T]): Gen[Attr[T]] = for {
  tid <- tupleId
  t   <- arb.arbitrary
} yield Attr(tid, t)

def attr[T](g: Gen[T]): Gen[Attr[T]] = for {
  tid <- tupleId
  t   <- g
} yield Attr(tid, t)
