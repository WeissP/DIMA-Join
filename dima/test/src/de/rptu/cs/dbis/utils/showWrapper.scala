package de.rptu.cs.dbis.utils.showWrapper

import cats.Show
import cats.syntax.all.*
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import org.scalacheck.Gen

case class S[T: Show](v: T) {
  override def toString(): String = v.show
}

object S {
  given [T: Show]: Show[ArrayBuffer[T]]       = Show.show(_.toVector.show)
  given [T: Show]: Show[Array[T]]             = Show.show(_.toVector.show)
  given [T: Show]: Show[mutable.ArraySeq[T]]  = Show.show(_.toVector.show)
  given [T: Show](using g: Gen[T]): Gen[S[T]] = for { v <- g } yield new S(v)
}
