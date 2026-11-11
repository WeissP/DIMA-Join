package de.rptu.cs.dbis.collection.scanmap

import cats.syntax.all.*
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.domain.HasTupleId
import de.rptu.cs.dbis.domain.MapElement
import scala.reflect.ClassTag
import de.rptu.cs.dbis.domain.interval.Interval
import de.rptu.cs.dbis.domain.interval.Bound
import de.rptu.cs.dbis.domain.CollectionStatistic
import de.rptu.cs.dbis.collection.bitset.Bitsets
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache

trait ScanMapClass[C, K, V, BS](using
  Ordering[K],
  ClassTag[BS],
  Bitset[BS],
  HasTupleId[V],
) extends Serializable {
  extension (m: C) {
    def insertSortedPairs[P](pairs: scala.collection.IndexedSeq[P])(using
      MapElement[P, K, V],
    ): Unit
    def filterValues(pred: V => Boolean): Unit
    def scan(k: K): Bitsets[BS] | Bitsets[BitsetWithBytesCache[BS]]
    def size: Int
    def minKey: Option[K]
    def maxKey: Option[K]
    def toMap: Map[K, Vector[V]]

    def keyInterval(using Numeric[K]): Option[Interval] = {
      (m.minKey, m.maxKey).mapN((min, max) =>
        Interval(Some(Bound.incl(min)), Some(Bound.incl(max))),
      )
    }

    def collectionStatistic(using num: Numeric[K]): CollectionStatistic = {
      CollectionStatistic(
        m.minKey.map(num.toDouble),
        m.maxKey.map(num.toDouble),
        size,
      )
    }

    def toSortedMap(using Ordering[V]): Map[K, Vector[V]] = {
      m.toMap.view.mapValues(v => v.sorted).toMap
    }
  }
}
