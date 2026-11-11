package de.rptu.cs.dbis.collection.scanmap

import cats.Show
import de.rptu.cs.dbis.collection.bitset.Bitset
import cats.syntax.all.*
import de.rptu.cs.dbis.collection.bitset.Bitsets
import scala.collection.IndexedSeq
import de.rptu.cs.dbis.domain.HasTupleId
import de.rptu.cs.dbis.domain.MapElement
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertIfDbg
import scala.reflect.ClassTag
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.utils.debug.isSorted
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache

opaque type SkipList[K, V, BS] = SkipListMapWithNull[K, V, BS]

object SkipList {
  extension [K, V, BS](slm: SkipList[K, V, BS]) {
    def config: SkipListConfig = slm.cfg
  }

  given [K, V, BS](using
    Numeric[K],
    ClassTag[K],
    ClassTag[BS],
    HasTupleId[V],
    Bitset[BS],
    Initable[Unit, BS],
  ): Initable[SkipListConfig, SkipList[K, V, BS]] with {
    extension (params: SkipListConfig) {
      def init: SkipList[K, V, BS] = { SkipListMapWithNull[K, V, BS](params) }
    }
  }

  given [K, V, BS](using
    Numeric[K],
    ClassTag[K],
    ClassTag[BS],
    HasTupleId[V],
    Bitset[BS],
    Initable[Unit, BS],
  ): Initable[(Config, ScanOperator), SkipList[K, V, BS]] with {
    extension (params: (Config, ScanOperator)) {
      def init: SkipList[K, V, BS] = {
        val slmCfg = SkipListConfig(
          includeEq = false,
          prob = Some(0.25f),
          bitsetMinLevelRatio = Some(0.1),
          expectElemNum =
            Some(params._1.windowSize.toInt / params._1.partitionNum),
          dispatcherNum = params._1.dispatcherNumber,
          scanLess = { params._2 == ScanOperator.Less },
        )
        slmCfg.init
      }
    }
  }

  given [K, V, BS](using num: Numeric[K])(using
    ClassTag[K],
    ClassTag[BS],
    HasTupleId[V],
    Bitset[BS],
  ): ScanMapClass[SkipList[K, V, BS], K, V, BS] with {
    extension (m: SkipList[K, V, BS]) {
      def insertSortedPairs[P](
        pairs: IndexedSeq[P],
      )(using MapElement[P, K, V]): Unit = {
        assertIfDbg(
          s"kv pairs to insert must be sorted by key, but got: ${pairs}",
          pairs.map(_.elemKey).toSeq.isSorted,
        )
        val toInsert = {
          if (m.scanLess) { pairs.iterator }
          else { pairs.reverseIterator.map(e => e.updateKey(num.negate)) }
        }
        m.addAllSorted(toInsert)
      }
      def filterValues(pred: V => Boolean): Unit = m.filterInPlace(pred)
      def scan(k: K): Bitsets[BS] | Bitsets[BitsetWithBytesCache[BS]] = {
        m.scanKeyLessThan { if (m.scanLess) k else num.negate(k) }
      }
      def size: Int = m.keyNum
      def minKey: Option[K] = {
        if (m.scanLess) { m.getMinKey }
        else { m.getMaxKey.map(num.negate) }
      }
      def maxKey: Option[K] = {
        if (m.scanLess) { m.getMaxKey }
        else { m.getMinKey.map(num.negate) }
      }
      def toMap: Map[K, Vector[V]] = {
        m.iterate.map(n => (n.k, n.values.toVector)).toMap
      }
    }
  }

  given [K: Show, V: Show, BS]: Show[SkipList[K, V, BS]] = Show.show(slm => {
    val nodes = slm.iterate.toVector

    (List(show"Config: ${slm.cfg}", s"Head Level: ${slm.headLevel}") ++
      (0 to slm.headLevel)
        .reverse
        .map(i => {
          val keys = slm.head.iterateAtLevel(i).map(_.k.show).mkString(" -> ")
          show"Level $i: $keys"
        })
        .appended {
          val values = nodes.map(_.values.mkString(",")).mkString(" -> ")
          show"Values: $values"
        }).mkString("\n")
  })
}
