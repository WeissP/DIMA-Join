package de.rptu.cs.dbis.collection.bitset

import com.zaxxer.sparsebits.SparseBitSet
import org.roaringbitmap.RoaringBitmapWriter
import org.roaringbitmap.buffer.MutableRoaringBitmap
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.domain.config.Config
import de.rptu.cs.dbis.domain.HasTupleId
import scala.reflect.ClassTag
import cats.syntax.all.*
import de.rptu.cs.dbis.domain.TupleId
import cats.Show
import org.roaringbitmap.RoaringBitmapWriter.Wizard
import org.roaringbitmap.buffer.MappeableContainer
import java.nio.ByteBuffer
import com.typesafe.scalalogging.StrictLogging

trait Bitset[C] extends Serializable {
  extension (bs: C) {
    def put(v: Int): Unit
    def remove(v: Int): Unit
    def union(o: C): Unit
    def intersect(o: C): Unit
    def foreach(f: Int => Boolean): Unit
    def isEmpty: Boolean
    def toBytes: Array[Byte]
  }
  def fromBytes(bytes: Array[Byte]): C
}

class BitsetWithBytesCache[BS](val bs: BS) extends Serializable {
  private var bytes = none[Array[Byte]]

  def clearCache               = { bytes = none }
  def setCache(c: Array[Byte]) = { bytes = Some(c) }
}

object BitsetWithBytesCache {
  given [BS](using bitset: Bitset[BS]): Bitset[BitsetWithBytesCache[BS]] with {

    extension (bc: BitsetWithBytesCache[BS]) {

      override def put(v: Int): Unit = {
        bc.clearCache
        bc.bs.put(v)
      }

      override def remove(v: Int): Unit = {
        bc.clearCache
        bc.bs.remove(v)
      }

      override def union(o: BitsetWithBytesCache[BS]): Unit = {
        bc.clearCache
        bc.bs.union(o.bs)
      }

      override def intersect(o: BitsetWithBytesCache[BS]): Unit = {
        bc.clearCache
        bc.bs.intersect(o.bs)
      }

      override def foreach(f: Int => Boolean): Unit = bc.bs.foreach(f)

      override def isEmpty: Boolean = bc.bs.isEmpty

      override def toBytes: Array[Byte] = {
        if (bc.bytes.isEmpty) { bc.setCache(bc.bs.toBytes) }
        bc.bytes.get
      }
    }

    override def fromBytes(bytes: Array[Byte]): BitsetWithBytesCache[BS] = {
      val bs = bitset.fromBytes(bytes)
      val bc = BitsetWithBytesCache[BS](bs)
      bc.setCache(bytes)
      bc
    }
  }
}

object Bitset {
  given Bitset[SparseBitSet] with {

    extension (bs: SparseBitSet) {

      override def put(v: Int): Unit = bs.set(v)

      override def remove(v: Int): Unit = bs.clear(v)

      override def union(o: SparseBitSet): Unit = bs.or(o)

      override def intersect(o: SparseBitSet): Unit = bs.and(o)

      override def foreach(f: Int => Boolean): Unit = {
        var cont         = true
        var current: Int = bs.nextSetBit(0)
        while (cont && current >= 0) {
          cont = f(current)
          current = bs.nextSetBit(current + 1)
        }
      }

      override def isEmpty: Boolean = bs.isEmpty()

      override def toBytes: Array[Byte] = {
        val buf = new java.io.ByteArrayOutputStream()
        bs.writeTo(new java.io.DataOutputStream(buf))
        buf.toByteArray.nn
      }
    }

    override def fromBytes(bytes: Array[Byte]): SparseBitSet = {
      val sbs = new SparseBitSet()
      sbs.readFrom(
        new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes)),
      )
      sbs
    }
  }

  given Bitset[MutableRoaringBitmap] with {

    extension (bs: MutableRoaringBitmap) {

      override def put(v: Int): Unit = bs.add(v)

      override def remove(v: Int): Unit = bs.remove(v)

      override def union(o: MutableRoaringBitmap): Unit = bs.or(o)

      override def intersect(o: MutableRoaringBitmap): Unit = bs.and(o)

      override def isEmpty: Boolean = bs.isEmpty()

      override def toBytes: Array[Byte] = {
        val buf = ByteBuffer.allocate(bs.serializedSizeInBytes())
        bs.serialize(buf)
        buf.nn.array().nn
      }

      override def foreach(f: Int => Boolean): Unit = {
        import scala.language.unsafeNulls

        val buffer = new Array[Int](1024)
        val it     = bs.getBatchIterator()
        var cont   = true

        while (cont && it.hasNext()) {
          val batch = it.nextBatch(buffer)
          var i     = 0
          while (cont && i < batch) {
            if (!f(buffer(i))) { cont = false }
            i += 1
          }
        }
      }
    }

    override def fromBytes(bytes: Array[Byte]): MutableRoaringBitmap = {
      val bm = MutableRoaringBitmap()
      bm.deserialize(ByteBuffer.wrap(bytes))
      bm
    }
  }
}

extension (r: MutableRoaringBitmap) {
  def toSet: Set[Int] = {
    import scala.collection.mutable

    val s: mutable.Set[Int] = mutable.Set()
    var it                  = r.getIntIterator().nn
    while (it.hasNext()) { s.add(it.next()) }
    s.toSet
  }
}

class Bitsets[BS](val size: Int)(using Initable[Unit, BS], ClassTag[BS])(using
  bs: Bitset[BS],
) extends Serializable with StrictLogging {
  val elems: Array[BS] = Array.fill(size)(().init)

  def isEmpty: Boolean = elems.forall(_.isEmpty)

  def or(o: Bitsets[BS]): Unit = {
    for (i <- 0 until size) { this.elems(i).union(o.elems(i)) }
  }

  def and(o: Bitsets[BS]): Unit = {
    for (i <- 0 until size) { this.elems(i).intersect(o.elems(i)) }
  }

  def set[O](t: O)(using HasTupleId[O]): Unit = {
    elems(t.dispatcherIdx).put(t.localId)
  }

  def setAll[O](ts: IterableOnce[O])(using HasTupleId[O]): Unit = {
    ts.iterator.foreach(set)
  }

  def remove[O](t: O)(using HasTupleId[O]): Unit = {
    elems(t.dispatcherIdx).remove(t.localId)
  }

  def storagePostions(cfg: Config): Bitsets[BS] = {
    summon[ClassTag[BS]].runtimeClass match {
      case c if c == classOf[MutableRoaringBitmap] => {
        def initWriter: RoaringBitmapWriter[MutableRoaringBitmap] = {
          RoaringBitmapWriter.bufferWriter().nn.optimiseForRuns().nn.get().nn
        }

        val writers = Array.fill(cfg.storageNumber)(initWriter)

        elems
          .view
          .zipWithIndex
          .foreach((bs, did) => {
            bs.foreach(lid => {
              val tid      = TupleId(did, lid)
              val sid      = cfg.getStorageIdx(tid)
              val storePos = cfg.getStorePostion(sid, lid)
              writers(sid).add(storePos)
              true
            })
          })

        val bss = new Bitsets[BS](cfg.storageNumber)
        for (i <- bss.elems.indices) {
          bss.elems(i) = writers(i).get().nn.asInstanceOf[BS]
        }
        bss
      }
      case c if c == classOf[SparseBitSet] => {
        val res = new Bitsets[BS](cfg.storageNumber)

        elems
          .view
          .zipWithIndex
          .foreach((bs, did) => {
            bs.foreach(lid => {
              val tid      = TupleId(did, lid)
              val sid      = cfg.getStorageIdx(tid)
              val storePos = cfg.getStorePostion(sid, tid.localId)
              res.elems(sid).put(storePos)
              true
            })
          })

        res
      }
      case _ =>
        throw RuntimeException(s"unexpected type: ${summon[ClassTag[BS]]}")
    }
  }

  def toBytes: Array[Byte] = {
    val buf = new java.io.ByteArrayOutputStream()
    val out = new java.io.DataOutputStream(buf)
    out.writeInt(elems.length)
    for (e <- elems) {
      val eBytes = e.toBytes
      out.writeInt(eBytes.length)
      out.write(eBytes)
    }
    buf.toByteArray.nn
  }

  override def toString(): String = elems
    .zipWithIndex
    .flatMap((bs, idx) => Option.unless(bs.isEmpty)(idx -> bs.toString()))
    .toMap
    .show
}

object Bitsets {
  given [BS]: Show[Bitsets[BS]] = Show.fromToString

  def fromIter[BS](
    size: Int,
    roaring_appender: => RoaringBitmapWriter[MutableRoaringBitmap],
    iter: IterableOnce[(Int, Int)],
  )(using Initable[Unit, BS], ClassTag[BS])(using Bitset[BS]): Bitsets[BS] = {
    summon[ClassTag[BS]].runtimeClass match {
      case c if c == classOf[MutableRoaringBitmap] => {
        val writers = Array.fill(size)(roaring_appender)
        for ((i, v) <- iter) { writers(i).add(v) }
        val bss = new Bitsets[BS](size)
        for (i <- bss.elems.indices) {
          bss.elems(i) = writers(i).get().nn.asInstanceOf[BS]
        }
        bss
      }
      case c if c == classOf[SparseBitSet] => {
        val bss = new Bitsets[BS](size)
        for ((i, v) <- iter) { bss.elems(i).put(v) }
        bss
      }
      case _ =>
        throw RuntimeException(s"unexpected type: ${summon[ClassTag[BS]]}")
    }
  }

  def fromBytes[BS](bytes: Array[Byte])(using Initable[Unit, BS], ClassTag[BS])(
    using bitset: Bitset[BS],
  ): Bitsets[BS] = {
    val in =
      new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes))
    val len = in.readInt()
    val bss = new Bitsets[BS](len)
    for (i <- 0 until len) {
      val eLen   = in.readInt()
      val eBytes = new Array[Byte](eLen)
      in.readFully(eBytes)
      bss.elems.update(i, bitset.fromBytes(eBytes))
    }
    bss
  }
}
