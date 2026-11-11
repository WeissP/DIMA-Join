package de.rptu.cs.dbis.collection.initable

import com.zaxxer.sparsebits.SparseBitSet
import org.roaringbitmap.buffer.MutableRoaringBitmap
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache

trait Initable[P, C] extends Serializable {
  extension (params: P) { def init: C }
}

object Initable {
  given Initable[Unit, SparseBitSet] with {
    extension (params: Unit) { def init: SparseBitSet = new SparseBitSet() }
  }

  given Initable[Unit, MutableRoaringBitmap] with {
    extension (params: Unit) {
      def init: MutableRoaringBitmap = new MutableRoaringBitmap()
    }
  }

  given [BS](using Initable[Unit, BS]): Initable[Unit, BitsetWithBytesCache[BS]]
  with {
    extension (params: Unit) {
      def init: BitsetWithBytesCache[BS] = BitsetWithBytesCache(().init)
    }
  }
}
