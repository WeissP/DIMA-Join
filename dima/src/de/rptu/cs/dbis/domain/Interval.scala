package de.rptu.cs.dbis.domain.interval

import cats.Show
import cats.derived.*
import cats.kernel.{Eq, Order}
import cats.syntax.all.*
import de.rptu.cs.dbis.services.DoubleOrdering
import de.rptu.cs.dbis.domain.ScanOperator

final case class Bound(value: Double, incl: Boolean) derives Eq, Show, Order {
  def switchIncl: Bound = Bound(value, !incl)
  def isBeforeBound[V: DoubleOrdering](v: V)(onEqual: Boolean): Boolean = {
    val cmp = v.compareToDouble(this.value)
    cmp == -1 || (cmp == 0 && onEqual)
  }
  def isAfterBound[V: DoubleOrdering](v: V)(onEqual: Boolean): Boolean = {
    val cmp = v.compareToDouble(this.value)
    cmp == 1 || (cmp == 0 && onEqual)
  }
}

object Bound {
  def excl[N: Numeric](v: N): Bound =
    Bound(Numeric[N].toDouble(v), incl = false)
  def incl[N: Numeric](v: N): Bound = Bound(Numeric[N].toDouble(v), incl = true)
}

/** Represents a mathematical interval with optional lower and upper bounds.
  * None for a bound represents infinity in that direction:
  */
final case class Interval(
  lower: Option[Bound] = None,
  upper: Option[Bound] = None,
) derives Eq, Order {
  override def toString: String = {
    val left     = lower.map(b => if (b.incl) "[" else "(").getOrElse("(")
    val right    = upper.map(b => if (b.incl) "]" else ")").getOrElse(")")
    val lowerVal = lower.map(_.value.toString).getOrElse("-∞")
    val upperVal = upper.map(_.value.toString).getOrElse("+∞")
    s"$left$lowerVal, $upperVal$right"
  }

  def withUpper(b: Bound): Interval = Interval(lower, Some(b))
  def withLower(b: Bound): Interval = Interval(Some(b), upper)

  def intersect(b: Interval): Interval = {
    val lower = (this.lower, b.lower) match {
      case (None, bLower) => bLower
      case (aLower, None) => aLower
      case (Some(aLower), Some(bLower)) =>
        if (aLower.value > bLower.value) aLower.some
        else if (aLower.value < bLower.value) bLower.some
        else aLower.copy(incl = aLower.incl && bLower.incl).some
    }

    val upper = (this.upper, b.upper) match {
      case (None, bUpper) => bUpper
      case (aUpper, None) => aUpper
      case (Some(aUpper), Some(bUpper)) =>
        if (aUpper.value < bUpper.value) aUpper.some
        else if (aUpper.value > bUpper.value) bUpper.some
        else aUpper.copy(incl = aUpper.incl && bUpper.incl).some
    }

    Interval(lower, upper)
  }

  def union(b: Interval): Interval = {
    val lower = (this.lower, b.lower) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(aLower), Some(bLower)) =>
        if (aLower.value < bLower.value) aLower.some
        else if (aLower.value > bLower.value) bLower.some
        else aLower.copy(incl = aLower.incl || bLower.incl).some
    }

    val upper = (this.upper, b.upper) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(aUpper), Some(bUpper)) =>
        if (aUpper.value > bUpper.value) aUpper.some
        else if (aUpper.value < bUpper.value) bUpper.some
        else aUpper.copy(incl = aUpper.incl || bUpper.incl).some
    }

    Interval(lower, upper)
  }

  def isBeforeLowerBound[V: DoubleOrdering](value: V): Boolean = {
    this.lower.map(lb => lb.isBeforeBound(value)(!lb.incl)).getOrElse(false)
  }

  def isAfterUpperBound[V: DoubleOrdering](value: V): Boolean = {
    this.upper.map(ub => ub.isAfterBound(value)(!ub.incl)).getOrElse(false)
  }

  def contains[V: DoubleOrdering](value: V): Boolean = {
    !isBeforeLowerBound(value) && !isAfterUpperBound(value)
  }

  def contains(other: Interval): Boolean = { this.union(other) === this }

  def scanIncluded[V: DoubleOrdering](
    value: V,
    scanOp: ScanOperator,
  ): Boolean = {
    scanOp match {
      case ScanOperator.Greater => this
          .upper
          .map(ub => ub.isBeforeBound(value)(ub.incl))
          .getOrElse(this.lower.isDefined)
      case ScanOperator.Less => this
          .lower
          .map(lb => lb.isAfterBound(value)(lb.incl))
          .getOrElse(this.upper.isDefined)
    }
  }

  def overlaps(b: Interval): Boolean = {
    val intersection = this.intersect(b)

    (intersection.lower, intersection.upper)
      .mapN((l, u) =>
        l.value < u.value || (l.value == u.value && l.incl && u.incl),
      )
      .getOrElse(true)
  }

  def shrink(percentage: Double, position: ShrinkPosition): Interval = {
    (this.lower, this.upper) match {
      case (Some(l), Some(u)) if percentage != 0 => {
        val delta = (u.value - l.value) * percentage

        position match {
          case ShrinkPosition.Left => this
              .copy(lower = Bound.incl(l.value + delta).some)
          case ShrinkPosition.Right => this
              .copy(upper = Bound.incl(u.value - delta).some)
          case ShrinkPosition.Middle => Interval(
              Bound.incl(l.value + delta / 2).some,
              Bound.incl(u.value - delta / 2).some,
            )
        }
      }
      case _ => this
    }
  }
}

object Interval {
  given Show[Interval] = Show.fromToString

}

enum ShrinkPosition {
  case Left
  case Right
  case Middle
}
