package de.rptu.cs.dbis.collection.scanmap

import de.rptu.cs.dbis.domain.MapElement
import de.rptu.cs.dbis.domain.HasTupleId
import de.rptu.cs.dbis.utils.debug.isSorted
import de.rptu.cs.dbis.utils.toOption
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertNoMsgsIfDbg
import scala.util.Random
import scala.math.Ordering
import cats.syntax.all.*
import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag
import cats.Show
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.IndexedSeq
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertIfDbg
import cats.derived.*
import de.rptu.cs.dbis.collection.bitset.Bitset
import de.rptu.cs.dbis.collection.bitset.Bitsets
import de.rptu.cs.dbis.collection.initable.Initable
import de.rptu.cs.dbis.collection.bitset.BitsetWithBytesCache

case class SkipListConfig(
  prob: Float,
  maxLevel: Int,
  includeEq: Boolean,
  bitsetMinLevel: Int,
  dispatcherNum: Int,
  validateCacheProp: Float,
  scanLess: Boolean,
) derives Show

object SkipListConfig {
  def apply(
    prob: Option[Float] = None,
    maxLevel: Option[Int] = None,
    includeEq: Boolean,
    expectElemNum: Option[Int] = None,
    bitsetMinLevel: Option[Int] = None,
    bitsetMinLevelRatio: Option[Double] = Some(0.5),
    dispatcherNum: Int,
    validateCacheProp: Option[Float] = None,
    scanLess: Boolean,
  ): SkipListConfig = {
    require(
      maxLevel.isDefined || expectElemNum.isDefined,
      "Either maxLevel or expectElemNum must be defined",
    )

    val p = prob.getOrElse(0.25f)
    val mlvl = maxLevel.getOrElse(math.max(
      1,
      math.round(math.log(expectElemNum.get.toDouble) / math.log(1.0 / p)).toInt,
    ))

    val bml = (bitsetMinLevel, bitsetMinLevelRatio) match {
      case (Some(l), _)    => l
      case (None, Some(r)) => (mlvl * r).toInt
      case _ => throw new IllegalArgumentException(
          "Either bitsetMinLevel or bitsetMinLevelRatio must be defined",
        )
    }

    require(mlvl > 0, s"maxLevel ${mlvl} must be greater than 1")

    new SkipListConfig(
      p,
      mlvl,
      includeEq,
      bml,
      dispatcherNum,
      validateCacheProp.getOrElse(0.2f),
      scanLess,
    )
  }
}

class SkipListMapWithNull[K, V, BS](val cfg: SkipListConfig)(using
  Ordering[K],
  ClassTag[K],
  ClassTag[BS],
  HasTupleId[V],
  Bitset[BS],
  Initable[Unit, BS],
) {
  val scanLess = cfg.scanLess

  type Node = Forwardable & BitsetCache
  type S    = Bitsets[BS]

  sealed trait Forwardable {
    val forward: Array[KeyValueNode | Null]
    def compareKey(o: Forwardable): Int

    def setNextNodeAtLvl(i: Int)(n: KeyValueNode | Null): Unit = {
      forward.update(i, n)
    }

    def nextNodeAtLvl(i: Int): KeyValueNode | Null = {
      if (i >= 0 && i < forward.length) forward(i) else null
    }

    def forwardInfo: String = {
      forward
        .map {
          case n if n != null => n.k.toString()
          case _              => "X"
        }
        .mkString(",")
    }

    def iterateAtLevel(level: Int): Iterator[KeyValueNode] = {
      new Iterator[KeyValueNode] {
        private var n: KeyValueNode | Null = nextNodeAtLvl(level)
        override def hasNext: Boolean      = n != null
        override def next(): KeyValueNode = {
          val cur = n
          if (cur == null) throw new NoSuchElementException()
          n = cur.nextNodeAtLvl(level)
          cur
        }
      }
    }

    def validate: Seq[String] = {
      val emptyIndices = forward
        .zipWithIndex
        .collect { case (n, i) if n == null => i }
      if (
        emptyIndices.isEmpty ||
        (emptyIndices.last - emptyIndices.head == emptyIndices.length - 1)
      ) Seq.empty
      else Seq(
        s"only the last n pointers can be empty but not in the middle, got emptyIndices: $emptyIndices",
      )
    }
  }

  trait BitsetCache extends Forwardable {
    def bitsetMaxLevel: Int
    val bitsetsArray: Array[S | Null]

    def getBitset(level: Int): S | Null = {
      bitsetsArray(level - cfg.bitsetMinLevel)
    }

    def clearBitset(level: Int): Unit = {
      bitsetsArray.update(level - cfg.bitsetMinLevel, null)
    }

    def putBitset(level: Int)(bs: S): Unit = {
      bitsetsArray.update(level - cfg.bitsetMinLevel, bs)
    }

    def createBitsetUpToKey(key: K): S = {
      val res = new S(cfg.dispatcherNum)
      this match {
        case kvn: KeyValueNode => kvn.values.foreach(res.set)
        case _                 => ()
      }

      iterateAtLevel(0).takeWhile(_.k < key).flatMap(_.values).foreach(res.set)
      res
    }

    def findHighestCacheUpToLevel(level: Int): Int = {
      var lvl = level
      while (lvl >= cfg.bitsetMinLevel) {
        if (getBitset(lvl) != null) return lvl
        lvl -= 1
      }
      -1
    }

    def setBitsetAtLevel(level: Int): Unit = {
      assertIfDbg(
        s"level ${level} must >= ${cfg.bitsetMinLevel}",
        level >= cfg.bitsetMinLevel,
      )
      val target = nextNodeAtLvl(level)
      if (target == null) {
        throw new RuntimeException(s"node at level $level doesn't exist")
      }
      val targetKey = target.k

      val bs = {
        if (level == cfg.bitsetMinLevel) { createBitsetUpToKey(targetKey) }
        else {
          assertIfDbg(
            s"bitset of level ${level - 1} must be set",
            getBitset(level - 1) != null,
          )

          val res = new S(cfg.dispatcherNum)
          res.or(getBitset(level - 1).nn)

          iterateAtLevel(level - 1)
            .takeWhile(_.k < targetKey)
            .foreach(n => {
              n.setBitsetUpToLevel(level - 1)
              val bss = n.getBitset(level - 1).nn
              for (i <- 0 to cfg.dispatcherNum) { res.or(bss) }
            })
          res
        }
      }
      putBitset(level)(bs)
    }

    def setBitsetUpToLevel(level: Int): Unit = {
      val highestCachedLevel = {
        val h = findHighestCacheUpToLevel(level)
        if (h >= 0) h else (cfg.bitsetMinLevel - 1)
      }

      for (i <- highestCachedLevel + 1 to level) { setBitsetAtLevel(i) }
    }

    def getOrCreateBitsetOfLevel(level: Int): S = {
      if (level < cfg.bitsetMinLevel) {
        val targetKey = nextNodeAtLvl(level).nn.k
        createBitsetUpToKey(targetKey)
      } else {
        val bs = getBitset(level)
        if (bs != null) bs.nn
        else {
          setBitsetUpToLevel(level)
          getBitset(level).nn
        }
      }
    }

    def setTupleIdFromLevelIfExists[O](t: O, startLevel: Int)(using
      HasTupleId[O],
    ) = {
      bitsetsArray
        .view
        .slice(startLevel - cfg.bitsetMinLevel, bitsetsArray.length)
        .foreach {
          case bs: S => bs.set(t)
          case _     => ()
        }
    }

    def removeTupleIdIfExists[O](t: O, startLevel: Int)(using HasTupleId[O]) = {
      bitsetsArray
        .view
        .slice(startLevel - cfg.bitsetMinLevel, bitsetsArray.length)
        .foreach {
          case bs: S => bs.remove(t)
          case _     => ()
        }
    }

    def clearBitsetIfExists(level: Int) = {
      if (level <= bitsetMaxLevel) { clearBitset(level) }
    }

    def bitsetInfo: String = {
      (Seq.fill(math.max(cfg.bitsetMinLevel - 1, 0))("X") ++
        bitsetsArray.map(bs => if (bs != null) bs.toString() else "X"))
        .mkString(",")
    }
  }

  final class ForwardNode extends Forwardable with BitsetCache {
    val forward        = Array.ofDim[KeyValueNode | Null](cfg.maxLevel + 1)
    def bitsetMaxLevel = headLevel
    val bitsetsArray = Array
      .ofDim[S | Null](cfg.maxLevel + 1 - cfg.bitsetMinLevel)

    def compareKey(o: Forwardable): Int = {
      o match {
        case _: KeyValueNode => -1
        case _               => 0
      }
    }

    def validateCache: Seq[String] = {
      val validateWholeBitsets = {
        val want = new S(cfg.dispatcherNum)
        iterateAtLevel(0).flatMap(_.values).foreach(want.set)
        if (want.toString() != wholeBitsets.toString()) {
          Seq(s"The whole bitset cache isn't correct. Want: ${want
              .toString()}, got: ${wholeBitsets.toString()}")
        }
        else Seq.empty
      }
      validateWholeBitsets ++ (cfg.bitsetMinLevel to bitsetMaxLevel)
        .flatMap(lvl => {
          val targetNode = nextNodeAtLvl(lvl)
          if (targetNode == null) Seq.empty
          else {
            val want = new S(cfg.dispatcherNum)
            iterateAtLevel(0)
              .takeWhile(_.k < targetNode.k)
              .flatMap(_.values)
              .foreach(want.set)
            val got = getBitset(lvl)
            if (got != null && want.toString() != got.toString()) {
              Seq(s"The bitset cache at level $lvl isn't correct. Want: ${want
                  .toString()}, got: ${got.toString()}")
            } else Seq.empty
          }
        })
    }

    override def validate: Seq[String] = {
      super.validate ++
        (if (rand.nextFloat() <= cfg.validateCacheProp) validateCache
         else Seq.empty)
    }

    override def toString(): String =
      s"ForwardNode(forward=$forwardInfo, bitset=$bitsetInfo)"
  }

  final class KeyValueNode(
    val k: K,
    v: V,
    val forward: Array[KeyValueNode | Null],
  ) extends Forwardable with BitsetCache {
    val maxLevel = forward.length - 1

    def bitsetMaxLevel: Int = maxLevel
    val bitsetsArray: Array[S | Null] = Array
      .ofDim[S | Null](cfg.maxLevel + 1 - cfg.bitsetMinLevel)

    def compareKey(o: Forwardable): Int = {
      o match {
        case kvn: KeyValueNode => Ordering[K].compare(k, kvn.k)
        case _                 => 1
      }
    }

    val values: ArrayBuffer[V] = ArrayBuffer(v)

    def insertValue(v: V): Unit = { values.addOne(v) }

    def validateCache: Seq[String] = {
      (cfg.bitsetMinLevel to bitsetMaxLevel).flatMap(lvl => {
        val targetNode = nextNodeAtLvl(lvl)
        if (targetNode == null) Seq.empty
        else {
          val want = new S(cfg.dispatcherNum)
          (values ++
            iterateAtLevel(0).takeWhile(_.k < targetNode.k).flatMap(_.values))
            .foreach(want.set)
          val got = getBitset(lvl)
          if (got != null && want.toString() != got.toString()) {
            Seq(s"The bitset cache at level $lvl isn't correct. Want: ${want
                .toString()}, got: ${got.toString()}")
          } else Seq.empty
        }
      })
    }

    override def validate: Seq[String] = {
      super.validate ++
        (if (values.nonEmpty) Seq.empty
         else Seq(s"Node with key $k has no values")) ++
        (if (rand.nextFloat() <= cfg.validateCacheProp) validateCache
         else Seq.empty)
    }

    override def toString(): String =
      s"KeyValueNode(key=$k, v=$values, next=[$forwardInfo], bitset=$bitsetInfo)"
  }

  extension (nodesWithIdx: Iterable[(Node, Int)]) {
    def onDistinctNodes(f: (Node, Int) => Unit): Unit = {
      var lastKey: K | Null = null
      nodesWithIdx.foreach((node, i) => {
        node match {
          case kvn: KeyValueNode if (lastKey != null && lastKey == kvn.k) => ()
          case kvn: KeyValueNode => {
            f(node, i)
            lastKey = kvn.k
          }
          case _ => { f(node, i) }
        }
      })
    }
  }

  extension (nodes: IndexedSeq[Node]) {
    def setTupleIdIfExistsFromLevel[O](
      startLevel: Int,
    )(t: O)(using HasTupleId[O]): Unit = {
      nodes
        .view
        .zipWithIndex
        .slice(startLevel, nodes.length)
        .onDistinctNodes((n, i) => n.setTupleIdFromLevelIfExists(t, i))
    }

    def removeTupleIdIfExistsFromLevel[O](
      startLevel: Int,
    )(t: O)(using HasTupleId[O]): Unit = {
      nodes
        .view
        .zipWithIndex
        .slice(startLevel, nodes.length)
        .onDistinctNodes((n, i) => n.removeTupleIdIfExists(t, i))
    }

    def clearBitsetUpToLevel(endLevel: Int) = {
      nodes
        .view
        .zipWithIndex
        .slice(cfg.bitsetMinLevel, endLevel + 1)
        .foreach((n, i) => n.clearBitset(i))
    }
  }

  def get(key: K): ArrayBuffer[V] | Null = {
    var cur = head

    for (i <- headLevel to 0 by -1) {
      var cont = true
      while (cont) {
        val n = cur.nextNodeAtLvl(i)
        if (n != null && n.k < key) cur = n else cont = false
      }
    }

    val n = cur.nextNodeAtLvl(0)
    if (n != null) n.values else null
  }

  def validateFoundClosestNodes(
    key: K,
    nodes: ArrayBuffer[Node],
  ): Seq[String] = {
    val sorted = nodes
      .reverse
      .sliding(2)
      .forall {
        case ArrayBuffer(p, n) => p.compareKey(n) <= 0
        case _                 => true
      }
    val lessThanTarget = nodes(0) match {
      case kvn: KeyValueNode => kvn.k < key
      case _                 => true
    }

    Option
      .unless(lessThanTarget && sorted) {
        val keys = nodes
          .map {
            case kvn: KeyValueNode => Some(kvn.k)
            case _                 => none
          }
          .mkString(",")
        s"found closest nodes are invalid, keys are: ${keys}"
      }
      .toSeq
  }

  def findClosestNodesOpBench(keys: Array[K]): Int = {
    var len   = 0
    val nodes = initNodes
    for (k <- keys) {
      findClosestNodes(k, nodes)
      len = nodes.length
    }
    len
  }

  def findClosestNodesBench(keys: Array[K]): Int = {
    var len = 0
    for (k <- keys) {
      val nodes = initNodes
      findClosestNodes(k, nodes)
      len = nodes.length
    }
    len
  }

  def findClosestNodes(key: K, nodes: ArrayBuffer[Node]): Unit = {
    assertNoMsgsIfDbg(validateFoundClosestNodes(key, nodes).map(s => {
      "Given nodes are invalid: " ++ s
    }))

    var cur = head

    (0 to headLevel)
      .reverse
      .foreach(i => {
        if (cur.compareKey(nodes(i)) < 0) { cur = nodes(i) }

        var cont = true
        while (cont) {
          val n = cur.nextNodeAtLvl(i)
          if (n != null && n.k < key) { cur = n }
          else { cont = false }
        }

        nodes.update(i, cur)
      })

    assertNoMsgsIfDbg(validateFoundClosestNodes(key, nodes))
  }

  val rand                     = new Random()
  var keyNum                   = 0
  var headLevel                = 0
  private var maxKey: K | Null = null
  val wholeBitsets = new Bitsets[BitsetWithBytesCache[BS]](cfg.dispatcherNum)

  val head: Node = new ForwardNode()

  def getMinKey: Option[K] = head.nextNodeAtLvl(0).toOption.map(_.k)
  def getMaxKey: Option[K] = maxKey.toOption

  def iterate: Iterator[KeyValueNode] = { head.iterateAtLevel(0) }

  def randomLevel(): Int = {
    var newLevel = 0
    while (rand.nextFloat() <= cfg.prob) { newLevel += 1 }
    math.min(newLevel, cfg.maxLevel)
  }

  def validate: Seq[String] = {
    val keysAtBaseLevel = iterate.map(_.k).toVector
    val keysOfAllLevels = (0 to headLevel)
      .flatMap(i => head.iterateAtLevel(i).map(_.k))
      .toSet

    head.validate ++ iterate.flatMap(n => n.validate) ++
      (if (keysAtBaseLevel.toSet == keysOfAllLevels) Seq.empty
       else Seq(
         s"keys at base level [$keysAtBaseLevel] != keys of all levels [$keysOfAllLevels]",
       )) ++
      (if (keysAtBaseLevel.isSorted) Seq.empty
       else Seq(s"keys are not sorted: $keysAtBaseLevel"))
  }

  def scanKeyLessThan(key: K): S | Bitsets[BitsetWithBytesCache[BS]] = {
    if (
      maxKey != null && (key > maxKey.nn || (cfg.includeEq && key == maxKey.nn))
    ) { return wholeBitsets }

    val res = new S(cfg.dispatcherNum)

    if (getMinKey.map(key < _).getOrElse(true)) { return res }

    var cur = head
    var lvl = headLevel

    while (lvl >= 0) {
      val next = cur.nextNodeAtLvl(lvl)
      if (next != null && next.k <= key) {
        res.or(cur.getOrCreateBitsetOfLevel(lvl))
        if (next.k == key) { lvl = -1 }
        cur = next
      } else { lvl -= 1 }
    }

    cur match {
      case lastNode: KeyValueNode
          if (lastNode.k < key || (cfg.includeEq && lastNode.k == key)) => {
        for (v <- lastNode.values) { res.set(v) }
      }
      case _ => ()
    }

    assertNoMsgsIfDbg(validate)
    res
  }

  def scanKeyLessThanWithoutCache(
    key: K,
  ): S | Bitsets[BitsetWithBytesCache[BS]] = {
    if (
      maxKey != null && (key > maxKey.nn || (cfg.includeEq && key == maxKey.nn))
    ) { return wholeBitsets }

    val res = new S(cfg.dispatcherNum)

    if (getMinKey.map(key < _).getOrElse(true)) { return res }

    iterate
      .takeWhile(n => n.k < key || (cfg.includeEq && n.k == key))
      .foreach(n => { n.values.foreach(v => { res.set(v) }) })

    res
  }

  def addAll(pairs: Iterable[(K, V)]): Unit = {
    pairs.foreach((k, v) => add(k, v))
  }

  def initNodes: ArrayBuffer[Node] = ArrayBuffer.fill(headLevel + 1)(head)
  def addAllSorted[P](pairs: Iterator[P])(using MapElement[P, K, V]): Unit = {
    val nodes = initNodes

    while (pairs.hasNext) {
      val p = pairs.next()
      addWithClosestNodes(p.elemKey, p.elemVal, nodes)
      if (!pairs.hasNext && (maxKey == null || maxKey.nn < p.elemKey)) {
        maxKey = p.elemKey
      }
    }
  }

  def addAllSortedWithoutCache[P](
    pairs: Iterator[P],
  )(using MapElement[P, K, V]): Unit = {
    val nodes = initNodes

    while (pairs.hasNext) {
      val p = pairs.next()
      addWithClosestNodesWithoutCache(p.elemKey, p.elemVal, nodes)
      if (!pairs.hasNext && (maxKey == null || maxKey.nn < p.elemKey)) {
        maxKey = p.elemKey
      }
    }
  }

  def add(key: K, v: V): Unit = {
    if (maxKey == null || maxKey.nn < key) { maxKey = key }
    addWithClosestNodes(key, v, initNodes)
  }

  def addWithClosestNodes(key: K, v: V, nodes: ArrayBuffer[Node]): Unit = {
    wholeBitsets.set(v)
    findClosestNodes(key, nodes)
    val n0 = nodes(0).nextNodeAtLvl(0)
    if (n0 != null && n0.k == key) {
      n0.insertValue(v)
      nodes
        .view
        .zipWithIndex
        .slice(math.max(n0.maxLevel + 1, cfg.bitsetMinLevel), nodes.length)
        .onDistinctNodes((n, i) => n.setTupleIdFromLevelIfExists(v, i))
      n0.setTupleIdFromLevelIfExists(v, cfg.bitsetMinLevel)
    } else {
      keyNum += 1
      val rlvl = randomLevel()
      if (rlvl > this.headLevel) {
        nodes.addAll(Iterator.fill(rlvl - this.headLevel)(head))
        this.headLevel = rlvl
      }

      val newNodeNext = nodes
        .view
        .take(rlvl + 1)
        .zipWithIndex
        .map((n, i) => n.nextNodeAtLvl(i))
        .toArray

      val newNode = new KeyValueNode(key, v, newNodeNext)

      (0 to rlvl).foreach(i => nodes(i).setNextNodeAtLvl(i)(newNode))

      nodes.clearBitsetUpToLevel(rlvl)
      nodes.setTupleIdIfExistsFromLevel(rlvl + 1)(v)
    }
    assertNoMsgsIfDbg(validate)
  }

  def addWithClosestNodesWithoutCache(
    key: K,
    v: V,
    nodes: ArrayBuffer[Node],
  ): Unit = {
    wholeBitsets.set(v)
    findClosestNodes(key, nodes)
    val n0 = nodes(0).nextNodeAtLvl(0)
    if (n0 != null && n0.k == key) { n0.insertValue(v) }
    else {
      keyNum += 1
      val rlvl = randomLevel()
      if (rlvl > this.headLevel) {
        nodes.addAll(Iterator.fill(rlvl - this.headLevel)(head))
        this.headLevel = rlvl
      }

      val newNodeNext = nodes
        .view
        .take(rlvl + 1)
        .zipWithIndex
        .map((n, i) => n.nextNodeAtLvl(i))
        .toArray

      val newNode = new KeyValueNode(key, v, newNodeNext)
      (0 to rlvl).foreach(i => nodes(i).setNextNodeAtLvl(i)(newNode))
    }
    assertNoMsgsIfDbg(validate)
  }

  def filterInPlace(pred: V => Boolean): Unit = {
    val lastNodes: Array[Node] = Array.fill(headLevel + 1)(head)

    var cur = head.nextNodeAtLvl(0)
    var lastKey = cur match {
      case v: KeyValueNode => v.k
      case _               => null
    }
    while (cur != null) {
      val node = cur
      node
        .values
        .view
        .filterNot(pred)
        .foreach(v => {
          wholeBitsets.remove(v)
          lastNodes
            .view
            .zipWithIndex
            .slice(
              math.max(node.maxLevel + 1, cfg.bitsetMinLevel),
              lastNodes.length,
            )
            .onDistinctNodes((n, i) => n.removeTupleIdIfExists(v, i))
          node.removeTupleIdIfExists(v, cfg.bitsetMinLevel)
        })

      node.values.filterInPlace(pred)
      if (node.values.isEmpty) {
        lastNodes.clearBitsetUpToLevel(node.maxLevel)
        for (i <- 0 to node.maxLevel) {
          lastNodes(i).setNextNodeAtLvl(i)(node.forward(i))
        }
        keyNum -= 1
      } else {
        for (i <- 0 to node.maxLevel) { lastNodes.update(i, node) }
        lastKey = node.k
      }
      cur = node.nextNodeAtLvl(0)
    }
    maxKey = if (keyNum == 0) null else lastKey
    while (head.nextNodeAtLvl(headLevel) == null && headLevel != 0) {
      headLevel -= 1
    }
    assertNoMsgsIfDbg(validate)
  }

  def filterInPlaceWithoutCache(pred: V => Boolean): Unit = {
    val lastNodes: Array[Node] = Array.fill(headLevel + 1)(head)

    var cur = head.nextNodeAtLvl(0)
    var lastKey = cur match {
      case v: KeyValueNode => v.k
      case _               => null
    }
    while (cur != null) {
      val node = cur

      node.values.view.filterNot(pred).foreach(v => { wholeBitsets.remove(v) })

      node.values.filterInPlace(pred)
      if (node.values.isEmpty) {
        for (i <- 0 to node.maxLevel) {
          lastNodes(i).setNextNodeAtLvl(i)(node.forward(i))
        }
        keyNum -= 1
      } else {
        for (i <- 0 to node.maxLevel) { lastNodes.update(i, node) }
        lastKey = node.k
      }
      cur = node.nextNodeAtLvl(0)
    }
    maxKey = if (keyNum == 0) null else lastKey
    while (head.nextNodeAtLvl(headLevel) == null && headLevel != 0) {
      headLevel -= 1
    }
    assertNoMsgsIfDbg(validate)
  }
}
