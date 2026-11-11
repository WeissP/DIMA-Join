package de.rptu.cs.dbis.collection.redblacktree

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeMap
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.scanmap.ScanMapClass
import de.rptu.cs.dbis.domain.HasTupleId
import scala.reflect.ClassTag
import de.rptu.cs.dbis.domain.MapElement
import de.rptu.cs.dbis.collection.bitset.Bitsets
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache

class RedBlackTree[K: Ordering, V, BS](
  val scanLess: Boolean,
  val dispatcherNum: Int,
)(using Initable[Unit, BS], Bitset[BS]) {
  val m: TreeMap[K, ArrayBuffer[V]] = TreeMap()
  val wholeBitsets = new Bitsets[BitsetWithBytesCache[BS]](dispatcherNum)

  def isMaxKey(k: K): Boolean = { m.minAfter(k).isDefined }
}

object RedBlackTree {
  given [K: Ordering, V, BS](using
    Initable[Unit, BS],
    Bitset[BS],
  ): Initable[(Config, ScanOperator), RedBlackTree[K, V, BS]] with {
    extension (params: (Config, ScanOperator)) {
      def init: RedBlackTree[K, V, BS] = new RedBlackTree(
        params._2 == ScanOperator.Less,
        params._1.dispatcherNumber,
      )
    }
  }

  given [K, V, BS](using
    Ordering[K],
    ClassTag[BS],
    Bitset[BS],
    HasTupleId[V],
    Initable[Unit, BS],
  ): ScanMapClass[RedBlackTree[K, V, BS], K, V, BS] with {
    extension (t: RedBlackTree[K, V, BS]) {
      def insertSortedPairs[P](
        pairs: scala.collection.IndexedSeq[P],
      )(using MapElement[P, K, V]): Unit = {
        for (p <- pairs) {
          t.m.getOrElseUpdate(p.elemKey, ArrayBuffer()).addOne(p.elemVal)
          t.wholeBitsets.set(p.elemVal)
        }
      }
      def filterValues(pred: V => Boolean): Unit = {
        t.m
          .filterInPlace((_, values) => {
            values.filterInPlace(pred)
            values
              .view
              .filterNot(pred)
              .foreach(v => { t.wholeBitsets.remove(v) })
            !values.isEmpty
          })
      }
      def scan(k: K): Bitsets[BS] | Bitsets[BitsetWithBytesCache[BS]] = {
        if (t.isMaxKey(k)) { return t.wholeBitsets }

        val bs = new Bitsets[BS](t.dispatcherNum)
        if (t.scanLess) { t.m.rangeUntil(k).flatMap(_._2).foreach(bs.set) }
        else { t.m.rangeFrom(k).flatMap(_._2).foreach(bs.set) }
        bs
      }
      def size: Int                = t.m.size
      def minKey: Option[K]        = t.m.keySet.minOption
      def maxKey: Option[K]        = t.m.keySet.maxOption
      def toMap: Map[K, Vector[V]] = t.m.view.mapValues(_.toVector).toMap
    }
  }
}
