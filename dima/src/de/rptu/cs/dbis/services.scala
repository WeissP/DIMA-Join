package de.rptu.cs.dbis.services

trait DoubleOrdering[T] {
  extension (t: T) def compareToDouble(d: Double): Int
}

object DoubleOrdering {

  given DoubleOrdering[Double] with {
    extension (v: Double) def compareToDouble(d: Double): Int = v.compare(d)
  }

  given [T](using num: Numeric[T]): DoubleOrdering[T] with {
    extension (v: T)
      def compareToDouble(d: Double): Int = num.toDouble(v).compare(d)
  }

}
