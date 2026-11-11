package de.rptu.cs.dbis.collection

import utest.*
import org.scalacheck.Gen
import cats.Show
import cats.syntax.all.*
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.utils.generator
import de.rptu.cs.dbis.utils.{toSet, checkSet}
import scala.collection.mutable.ArrayBuffer
import org.scalacheck.Shrink
import de.rptu.cs.dbis.domain.TupleId
import com.zaxxer.sparsebits.SparseBitSet
import scala.util.Random
import de.rptu.cs.dbis.domain.HasTupleId
import scala.collection.mutable.{TreeMap as ScalaTreeMap}
import cats.kernel.{Eq, Order}
import cats.derived.*
import de.rptu.cs.dbis.utils.PropCheckByGenSuite
import de.rptu.cs.dbis.collection.scanmap.SkipList
import de.rptu.cs.dbis.collection.scanmap.SkipListConfig
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.bitset.Bitsets
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache
import de.rptu.cs.dbis.domain.MapElement
import scala.reflect.ClassTag

type BS = Bitsets[SparseBitSet] | Bitsets[BitsetWithBytesCache[SparseBitSet]]

extension (res: BS) {
  def sparseElems: Array[SparseBitSet] = res.elems match {
    case arr: Array[SparseBitSet] @unchecked => arr
    case arr: Array[BitsetWithBytesCache[SparseBitSet]] @unchecked =>
      arr.map(_.bs)
  }
}

case class Value(timestamp: Int, tupleId: TupleId) derives Eq, Show, Order
object Value {
  given Ordering[Value] = Order[Value].toOrdering
  given HasTupleId[Value] with {
    extension (o: Value) def tupleId: TupleId = o.tupleId
  }
}

case class InputGenSuit(
  partitionRatio: Double,
  windowRatio: Double,
  kvPairs: Vector[(Double, Value)],
) derives Show {
  val chunkSize = math.max(1, (kvPairs.length * partitionRatio).toInt)
  val chunked   = kvPairs.grouped(chunkSize).toList
  val window    = math.max(1, (kvPairs.length * windowRatio).toInt)
}

object InputGenSuit {
  given Gen[InputGenSuit] = for {
    partitionRatio <- Gen.choose(0.1, 1.0)
    windowRatio    <- Gen.choose(0.1, 0.8)
    uniqueNum      <- Gen.choose(1, 1000)
    dupsNum        <- Gen.choose(0, uniqueNum)
    totalNum = uniqueNum + dupsNum
    uniqueKeys <- Gen.listOfN(uniqueNum, generator.double)
    keys   = Random.shuffle(uniqueKeys ++ uniqueKeys.take(dupsNum))
    values = (0 to totalNum).map(i => Value(i, TupleId(0, i)))
  } yield InputGenSuit(partitionRatio, windowRatio, keys.lazyZip(values).toVector)

  given Shrink[InputGenSuit] = Shrink(gs => {
    (1 until gs.kvPairs.length)
      .toStream
      .map(i => gs.copy(kvPairs = gs.kvPairs.dropRight(i)))
  })
}

case class SkipListMapGensuit(
  slm: SkipList[Double, TupleId, SparseBitSet],
  map: Map[Double, Vector[TupleId]],
  prob: Float,
) {
  override def toString(): String =
    show"Skip List:\n$slm\nmap:$map\nprob:$prob\n"
}

object SkipListMapGensuit {
  def genSkipListByMap(
    map: Map[Double, Vector[TupleId]],
    prob: Float,
    scanLess: Boolean,
  ): SkipList[Double, TupleId, SparseBitSet] = {
    val slmCfg = SkipListConfig(
      prob = Some(prob),
      includeEq = false,
      expectElemNum = Some(map.size),
      dispatcherNum = generator.Constants.dispatcherNum,
      scanLess = scanLess,
    )

    val x =
      summon[Initable[SkipListConfig, SkipList[Double, TupleId, SparseBitSet]]]
    val slm = x.init(slmCfg)
    val kvPairs = map
      .iterator
      .flatMap((k, vs) => vs.map((k, _)))
      .toVector
      .sorted
    slm.insertSortedPairs(kvPairs)
    slm
  }

  def gen(scanLess: Boolean): Gen[SkipListMapGensuit] = for {
    keys <- generator.doubleSet
    values <- Gen.listOfN(
      keys.size,
      Gen.nonEmptyListOf(generator.tupleId).map(v => v.distinct.sorted.toVector),
    )
    map = keys.zip(values.map(_.toVector)).toMap
    prob <- Gen.choose(0.1, 0.9).map(_.toFloat)
  } yield SkipListMapGensuit(genSkipListByMap(map, prob, scanLess), map, prob)

  given genScanLess: Gen[SkipListMapGensuit]    = gen(true)
  given genScanGreater: Gen[SkipListMapGensuit] = gen(false)
  given genScanRandom: Gen[SkipListMapGensuit] = Gen.prob(0.5).flatMap(this.gen)

  given Shrink[SkipListMapGensuit] = Shrink(gs => {
    val shrunkMap = Shrink
      .shrinkContainer2[Map, Double, Vector[TupleId]]
      .suchThat(m => m.values.forall(v => !v.isEmpty))
      .shrink(gs.map)

    shrunkMap.map(m => {
      SkipListMapGensuit(
        genSkipListByMap(m, gs.prob, gs.slm.config.scanLess),
        m,
        gs.prob,
      )
    }) ++ shrunkMap.map(m => {
      SkipListMapGensuit(genSkipListByMap(m, 0, gs.slm.config.scanLess), m, 0)
    })
  })
}

def checkCachedStatistic(
  slm: SkipList[Double, TupleId, SparseBitSet],
  want: Map[Double, Vector[TupleId]],
): List[(String, Boolean)] = {
  val gotMin   = slm.minKey
  val gotMax   = slm.maxKey
  val gotSize  = slm.size
  val wantMin  = want.keys.minOption
  val wantMax  = want.keys.maxOption
  val wantSize = want.keys.size

  val dbgStr =
    show"\nGot map: ${slm.toSortedMap}, want map: ${want}, skiplist:\n${slm}"

  List(
    show"min key isn't correct. Got: ${gotMin}, want: ${wantMin}${dbgStr}" ->
      (gotMin == wantMin),
    show"max key isn't correct. Got: ${gotMax}, want: ${wantMax}${dbgStr}" ->
      (gotMax == wantMax),
    show"size isn't correct. Got: ${gotSize}, want: ${wantSize}${dbgStr}" ->
      (gotSize == wantSize),
  )
}

object SkipListMapTest extends TestSuite {
  override def tests: Tests = Tests {
    test("SkipList insert") {
      new PropCheckByGenSuite[SkipListMapGensuit](using
        SkipListMapGensuit.genScanLess,
      ) {
        override def checkProp(gs: SkipListMapGensuit) = {
          val got = gs.slm.toSortedMap

          List(show"insert isn't correct. Got: ${got}, want: ${gs
              .map}, skiplist:\n${gs.slm}" -> (got == gs.map)) ++
            checkCachedStatistic(gs.slm, gs.map)
        }
      }.execute
    }

    test("SkipList addAllSorted") {
      new PropCheckByGenSuite[SkipListMapGensuit](using
        SkipListMapGensuit.genScanLess,
      ) {
        override def checkProp(gs: SkipListMapGensuit) = {
          val kvPairs = gs
            .map
            .iterator
            .toSeq
            .sorted
            .flatMap((k, vs) => vs.map((k, _)))
            .toVector

          val slmCfg = gs.slm.config
          val slm = summon[
            Initable[SkipListConfig, SkipList[Double, TupleId, SparseBitSet]],
          ].init(slmCfg)
          slm.insertSortedPairs(kvPairs)
          val got = slm.toSortedMap

          List(
            show"addAllSorted isn't correct. Got: ${got}, want: ${gs.map}, skiplist:\n${slm}" ->
              (got == gs.map),
          ) ++ checkCachedStatistic(gs.slm, gs.map)
        }
      }.execute
    }

    test("SkipList clean") {
      given Gen[(SkipListMapGensuit, Int)] = Gen
        .zip(SkipListMapGensuit.genScanLess, generator.tupleId.map(_.localId))

      given Shrink[(SkipListMapGensuit, Int)] = Shrink(gs => {
        val shrunkMap = summon[Shrink[SkipListMapGensuit]].shrink(gs._1)
        shrunkMap.map(m => (m, gs._2))
      })

      new PropCheckByGenSuite[(SkipListMapGensuit, Int)] {
        override val minSuccessfulTests: Int = 500
        override def checkProp(in: (SkipListMapGensuit, Int)) = {
          val gs               = in._1
          val thres            = in._2
          def pred(i: TupleId) = i.localId > thres
          gs.slm.filterValues(pred)
          val got = gs.slm.toSortedMap
          val want = gs
            .map
            .view
            .mapValues(_.filter(pred))
            .filter((_, v) => !v.isEmpty)
            .toMap

          List(
            "no exception" -> true,
            show"clean isn't correct. Got: ${got}, want: ${want}, skiplist:\n${gs.slm}" ->
              (got == want),
          ) ++ checkCachedStatistic(gs.slm, want)
        }
      }.execute
    }

    test("SkipList scan") {
      given Gen[(SkipListMapGensuit, List[Double])] = Gen.zip(
        SkipListMapGensuit.genScanRandom,
        Gen.listOfN(
          50,
          Gen.choose(
            -2 * generator.Constants.maxDouble,
            2 * generator.Constants.maxDouble,
          ),
        ),
      )

      new PropCheckByGenSuite[(SkipListMapGensuit, List[Double])] {
        override def checkProp(in: (SkipListMapGensuit, List[Double])) = {

          val gs   = in._1
          val keys = in._1.map.keySet.toList ++ in._2

          val mapBeforeScan = gs.slm.toSortedMap

          val scanRes = keys.map(key => {
            val got = gs
              .slm
              .scan(key)
              .sparseElems
              .zipWithIndex
              .flatMap((m, i) => Option.unless(m.isEmpty())(i -> m))
              .toMap
            val want = gs
              .map
              .view
              .filterKeys(k => {
                if (gs.slm.config.scanLess) { k < key }
                else { k > key }
              })
              .values
              .flatten
              .groupBy(_.dispatcherIdx)
              .view
              .mapValues(tids => {
                val res = new SparseBitSet()
                for (t <- tids) { res.set(t.localId) }
                res
              })
              .toMap
            (
              s"scan isn't correct for key ${key}. Got: ${got}, want: ${want}. Skiplist:\n${gs.slm.show}\n",
              got == want,
            )
          })

          val mapAfterScan = gs.slm.toSortedMap
          scanRes :+
            ("Skip list changes after scaning", mapBeforeScan === mapAfterScan)
        }
      }.execute
    }

    test("SkipList combined") {
      new PropCheckByGenSuite[InputGenSuit] {
        def checkProp(gs: InputGenSuit): List[(String, Boolean)] = {
          val slmCfg = SkipListConfig(
            includeEq = false,
            expectElemNum = Some(gs.kvPairs.length),
            dispatcherNum = 1,
            scanLess = true,
            bitsetMinLevel = Some(0),
            validateCacheProp = Some(0.8f),
          )

          val skipListMap: SkipList[Double, Value, SparseBitSet] = summon[
            Initable[SkipListConfig, SkipList[Double, Value, SparseBitSet]],
          ].init(slmCfg)
          val scalaTreeMap = ScalaTreeMap[Double, ArrayBuffer[Value]]()

          def check(
            msgPrefix: String,
            slm: SkipList[Double, Value, SparseBitSet],
            m: ScalaTreeMap[Double, ArrayBuffer[Value]],
          ): List[(String, Boolean)] = {
            val mMap   = m.toMap.view.mapValues(vs => vs.toVector.sorted).toMap
            val slmMap = slm.toSortedMap
            checkSet("SkipList", slmMap.keySet)("Want", mMap.keySet)
              .map((msg, b) =>
                (show"$msgPrefix: $msg\nskip list: $slm\nwant: $mMap", b),
              )
              .appended("Result dismatch" -> (slmMap === mMap))
          }

          gs.chunked
            .zipWithIndex
            .map((unsort, idx) => {
              val chunk = unsort.sorted
              skipListMap.insertSortedPairs(chunk)
              for ((k, v) <- chunk) {
                scalaTreeMap.getOrElseUpdate(k, ArrayBuffer()).addOne(v)
              }
              val insertRes =
                check(s"Chunk $idx: After inserting", skipListMap, scalaTreeMap)
              val scanRes = unsort.flatMap((k, _) => {
                val slmRes =
                  skipListMap.scan(k).sparseElems.map(_.toSet).toList
                val mapRes = {
                  val res = new Bitsets[SparseBitSet](1)
                  scalaTreeMap.rangeUntil(k).flatMap(_._2).foreach(res.set)
                  res.elems.map(_.toSet).toList
                }
                checkSet("SkipList", slmRes(0))("Want", mapRes(0))
                  .map((msg, b) => (s"Chunk $idx: scan on key $k: $msg", b))
              })
              val expireTime = chunk.last._2.timestamp - gs.window

              skipListMap.filterValues(v => v.timestamp >= expireTime)
              scalaTreeMap.filterInPlace((_, values) => {
                values.filterInPlace(v => v.timestamp >= expireTime)
                !values.isEmpty
              })
              val expireRes = check(
                s"Chunk $idx: After expiring key < ${expireTime}",
                skipListMap,
                scalaTreeMap,
              )
              insertRes ++ expireRes ++ scanRes
            })
            .flatten
            .toList
        }

      }.execute

    }
  }
}
