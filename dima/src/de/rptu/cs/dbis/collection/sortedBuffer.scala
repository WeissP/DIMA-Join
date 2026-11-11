package de.rptu.cs.dbis.collection.sortedBuffer

import scala.math.Ordered.orderingToOrdered
import scala.collection.IndexedSeqView
import scala.reflect.ClassTag
import scala.collection.mutable.IndexedBuffer
import de.rptu.cs.dbis.domain.ScanOperator
import scala.collection.Searching.SearchResult
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertIfDbg
import de.rptu.cs.dbis.utils.debug.isSorted

enum DuplicatePolicy {
  case Any, LeftMost, RightMost
}

extension [T](sb: IndexedBuffer[T])(using ClassTag[T]) {
  def sortedInsert(v: T)(using Ordering[T]): Unit = {
    sb.insert(sb.search(v).insertionPoint, v)
    assertIfDbg("buffer is sorted", sb.isSorted)
  }

  /** Performs a binary search on an sorted sequence to find a target element.
    *
    * @param elem The double value to search for in the sequence
    * @param policy Specifies how to handle duplicate elements:
    *   - Any: Returns any matching index (fastest)
    *   - LeftMost: Returns the leftmost matching index using linear search
    *   - RightMost: Returns the rightmost matching index using linear search
    * @return The SearchResult of the element
    */
  def binarySearch(elem: T, policy: DuplicatePolicy = DuplicatePolicy.Any)(using
    Ordering[T],
  ): SearchResult = {
    @scala.annotation.tailrec
    def loop(from: Int, to: Int): Int = {
      if (from < 0) loop(0, to)
      else if (to > sb.length) loop(from, sb.length)
      else if (to <= from) from
      else {
        val idx = from + (to - from - 1) / 2

        sb(idx).compare(elem) match {
          case 1  => loop(from, idx)
          case -1 => loop(idx + 1, to)
          case 0 => policy match {
              case DuplicatePolicy.Any      => idx
              case DuplicatePolicy.LeftMost =>
                // Linear search leftward
                var i = idx
                while (i > 0 && sb(i - 1).compare(elem) == 0) { i -= 1 }
                i
              case DuplicatePolicy.RightMost =>
                // Linear search rightward
                var i = idx
                while (i < sb.length - 1 && sb(i + 1).compare(elem) == 0) {
                  i += 1
                }
                i
            }
        }
      }
    }
    val res = loop(0, sb.length)
    if (res < sb.length && sb(res).compare(elem) == 0) Found(res)
    else InsertionPoint(res)
  }

  def lessThan(k: T)(using Ordering[T]): IndexedSeqView[T] = {
    if (sb.isEmpty || sb.head.compare(k) == 1) { Array[T]().view }
    else if (sb.last.compare(k) == -1) { sb.view }
    else {
      val pos = sb.binarySearch(k, DuplicatePolicy.LeftMost).insertionPoint
      sb.view.slice(0, pos)
    }
  }

  def greaterThan(k: T)(using Ordering[T]): IndexedSeqView[T] = {
    if (sb.isEmpty || sb.last.compare(k) == -1) { Array[T]().view }
    else if (sb.head.compare(k) == 1) { sb.view }
    else {
      val pos = sb.binarySearch(k, DuplicatePolicy.RightMost) match {
        case Found(foundIndex)              => foundIndex + 1
        case InsertionPoint(insertionPoint) => insertionPoint
      }
      sb.view.slice(pos, sb.length)
    }
  }

  def scanValue(k: T, scanOp: ScanOperator)(using
    Ordering[T],
  ): IndexedSeqView[T] = {
    scanOp match {
      case ScanOperator.Greater => greaterThan(k)
      case ScanOperator.Less    => lessThan(k)
    }
  }
}
