package de.rptu.cs.dbis.collection.sortedBuffer

import scala.collection.mutable.ArrayBuffer
import utest.*
import org.scalacheck.Gen
import cats.Show
import cats.syntax.all.*
import de.rptu.cs.dbis.utils.generator
import de.rptu.cs.dbis.utils.PropCheckByGenSuite

case class SortedBufferGensuit(key: Double, buf: ArrayBuffer[Double])

object SortedBufferGensuit {
  given Gen[SortedBufferGensuit] = for {
    vs <- Gen.nonEmptyListOf(generator.double)
    key <- Gen.frequency(
      1 -> vs.head,
      1 -> generator.double,
      5 -> Gen.choose(vs.min, vs.max),
    )
  } yield SortedBufferGensuit(key, ArrayBuffer.from(key :: vs ::: vs ::: vs).sorted)
}

object SortedBufferTest extends TestSuite {
  override def tests: Tests = Tests {
    test("SortedBuffer scan less") {
      new PropCheckByGenSuite[SortedBufferGensuit] {
        override val minSuccessfulTests: Int = 500
        override def checkProp(gs: SortedBufferGensuit) = {
          val want = gs.buf.filter(_ < gs.key).toList
          val got  = gs.buf.lessThan(gs.key).toList

          List(
            show"scan less isn't correct. Got: ${got}, want: ${want}" ->
              (want == got),
          )
        }
      }.execute
    }
    test("SortedBuffer scan greater") {
      new PropCheckByGenSuite[SortedBufferGensuit] {
        override val minSuccessfulTests: Int = 500
        override def checkProp(gs: SortedBufferGensuit) = {
          val want = gs.buf.filter(_ > gs.key).toList
          val got  = gs.buf.greaterThan(gs.key).toList

          List(
            show"scan greater isn't correct. Got: ${got}, want: ${want}" ->
              (want == got),
          )
        }
      }.execute
    }
  }
}
