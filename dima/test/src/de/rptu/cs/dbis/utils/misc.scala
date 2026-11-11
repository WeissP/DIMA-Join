package de.rptu.cs.dbis.utils

import cats.Show
import cats.syntax.all.*
import com.zaxxer.sparsebits.SparseBitSet
import de.rptu.cs.dbis.domain.interval.Interval
import de.rptu.cs.dbis.domain.interval.Bound

extension [A](seq: Iterable[A])(using Show[A]) {
  def report(sep: String = "\n") = {
    "[\n" ++ seq.map(_.show).mkString(sep) ++ "\n]"
  }
}

extension [A](seq: Iterable[A]) {
  def <==>(other: Iterable[A])(implicit eq: cats.kernel.Eq[A]): Boolean =
    unorderedEq(other)

  def unorderedEq(
    other: Iterable[A],
  )(implicit eq: cats.kernel.Eq[A]): Boolean = {
    seq.size === other.size &&
    seq.forall(a => seq.count(eq.eqv(a, _)) === other.count(eq.eqv(a, _)))
  }
}

def reportParams(ps: (String, Any)*): String = {
  ps.map((name, v) => s"${name}: ${v}").mkString("\n")
}

object DbgShow {
  given Show[Double]                   = Show.show(v => f"$v%.2f")
  given [V: Show]: Show[IndexedSeq[V]] = Show.show(v => v.toList.show)
}

extension (bs: SparseBitSet) {
  def toSet: Set[Int] = {
    val iter = new Iterator[Int] {
      private var current: Int      = bs.nextSetBit(0)
      override def hasNext: Boolean = current >= 0
      override def next(): Int = {
        val result = current
        current = bs.nextSetBit(current + 1)
        result
      }
    }
    iter.toSet
  }
}

def checkSet[V](
  xName: String,
  x: Set[V],
)(yName: String, y: Set[V]): List[(String, Boolean)] = {
  val moreOnX = x.diff(y)
  val moreOnY = y.diff(x)

  List(
    s"More elems on $xName: $moreOnX" -> moreOnX.isEmpty,
    s"More elems on $yName: $moreOnY" -> moreOnY.isEmpty,
  )
}

extension (x: Option[Bound]) {

  /** Compare two potential lower bounds to determine their ordering
    *
    * @param y
    *   The other lower bound to compare against
    * @return
    *   True if x represents a strictly lesser lower bound, considering:
    *   - A bound with smaller value is always "less"
    *   - For equal values, an inclusive bound is considered "less" than
    *     exclusive
    *   - None (unbounded) is considered negative infinity (always least
    *     possible)
    */
  def cmpAsLower(y: Option[Bound]): Int = (x, y) match {
    case (None, None)    => 0
    case (None, Some(_)) => -1
    case (Some(_), None) => 1
    case (Some(a), Some(b)) =>
      if (a.value < b.value) -1
      else if (a.value > b.value) 1
      else if (a.incl == b.incl) 0
      else if (a.incl) -1 // a is inclusive, b is exclusive
      else 1              // a is exclusive, b is inclusive
  }

  /** Compare two potential upper bounds to determine their ordering
    *
    * @param y
    *   The other upper bound to compare against
    * @return
    *   Comparison result considering:
    *   - A bound with larger value is always "greater"
    *   - For equal values, an inclusive bound is considered "greater" than
    *     exclusive
    *   - None (unbounded) is considered positive infinity (always greatest
    *     possible)
    */
  def cmpAsUpper(y: Option[Bound]): Int = (x, y) match {
    case (None, None)    => 0
    case (None, Some(_)) => 1
    case (Some(_), None) => -1
    case (Some(a), Some(b)) =>
      if (a.value > b.value) 1
      else if (a.value < b.value) -1
      else if (a.incl == b.incl) 0
      else if (a.incl) 1 // a is inclusive, b is exclusive
      else -1            // a is exclusive, b is inclusive
  }
}

extension (interval: Interval) {

  /** Check if this interval is completely contained within another interval
    *
    * @param other
    *   The interval to check containment against
    * @return
    *   True if both:
    *   - This interval's lower bound is not less than the other's lower bound
    *   - This interval's upper bound is not greater than the other's upper
    *     bound
    */
  def isIncludedIn(other: Interval): Boolean = {
    (interval.lower.cmpAsLower(other.lower) >= 0) &&
    (interval.upper.cmpAsUpper(other.upper) <= 0)
  }
}
