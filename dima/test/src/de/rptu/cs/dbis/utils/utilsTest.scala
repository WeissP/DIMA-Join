package de.rptu.cs.dbis.utils

import utest.*
import org.scalacheck.Gen
import cats.Show
import cats.syntax.all.*
import de.rptu.cs.dbis.utils.toSet
import org.scalacheck.Shrink
import com.zaxxer.sparsebits.SparseBitSet
import scala.util.Try
import scala.util.Success

class GenSuit(val intSet: Set[Int]) {
  val sbs = new SparseBitSet()
  intSet.foreach(sbs.set)

  override def toString(): String = intSet.show
}

object GenSuit {
  given Gen[GenSuit] = generator.posIntSet.map(GenSuit.apply)
}

object SparseBitSetTest extends TestSuite {
  override def tests: Tests = Tests {
    test("SparseBitSet") {
      new PropCheckByGenSuite[GenSuit] {
        def checkProp(gs: GenSuit): List[(String, Boolean)] = {
          val gotFromString = Try(gs.sbs.toSet)
          val gotFromIter   = gs.sbs.toSet
          List(
            s"SparseBitSet toSet error: ${gs.sbs}" ->
              (gotFromString == Success(gs.intSet)),
            s"SparseBitSet toSet error: ${gs.sbs}" -> (gotFromIter == gs.intSet),
          )
        }
      }.execute
    }
  }
}
