package de.rptu.cs.dbis.domain.config

import cats.Show
import cats.derived.*
import cats.kernel.Eq
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.clock.*
import de.rptu.cs.dbis.utils.debug.BuildEnv.assertEqIfDbg
import scala.collection.immutable.ArraySeq
import de.rptu.cs.dbis.domain.tuples.shuffler.spo.SpoStream
import de.rptu.cs.dbis.domain.tuples.shuffler.SpoutTupleStream
import de.rptu.cs.dbis.domain.tuples.shuffler.NycStream
import de.rptu.cs.dbis.domain.tuples.shuffler.taq.TaqStream
import de.rptu.cs.dbis.domain.tuples.shuffler.blond.BlondStream
import de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer.HouseBuyerQuery

final case class IndexerCfg(
  bufferCap: Option[Int] = None,
  partitionNum: Option[Int] = None,
  initialBufferCap: Option[Int] = None,
) derives Show, Eq

final case class Config(
  partitionNum: Int,
  partitionTaskNum: Option[Int],
  windowSize: Long,
  bufferCap: Int,
  initialBufferCap: Int,
  maxDistance: Double,
  dispatcherNumber: Int,
  storageNumber: Int,
  sampleStorageNumPerDispatcher: Int,
  expireInterval: Int,
  joinerNumber: Int,
  attrNames: ArraySeq[String],
  attrQueryNum: Int,
  countBased: Boolean,
  maxSpoutPending: Option[Int],
  numWorkers: Int,
  trackTime: Boolean,
  trackTimeIntervalMillis: Long,
  emitProb: Float,
  indexerCfgs: Map[String, IndexerCfg] = Map.empty,
  ackerNumber: Int = 4,
) derives Show, Eq {
  require(attrQueryNum > 0, s"attrQueryNum $attrQueryNum must be positive")
  require(partitionNum > 0, s"partitionNum $partitionNum must be positive")
  require(joinerNumber > 0, s"joinerNumber $joinerNumber must be positive")
  require(windowSize > 0, s"windowSize $windowSize must be positive")
  require(bufferCap > 0, s"bufferCap $bufferCap must be positive")
  require(
    initialBufferCap >= bufferCap,
    s"initialBufferCap $initialBufferCap must be >= bufferCap $bufferCap",
  )
  require(maxDistance > 0, s"maxDistance $maxDistance must be positive")
  require(
    expireInterval > 0,
    s"expireIntervalSeconds $expireInterval must be positive",
  )
  require(
    dispatcherNumber > 0,
    s"dispatcherNumber $dispatcherNumber must be positive",
  )
  require(storageNumber > 0, s"storageNumber $storageNumber must be positive")
  require(
    storageNumber % dispatcherNumber == 0,
    s"storageNumber $storageNumber must be a multiple of dispatcherNumber $dispatcherNumber",
  )
  require(
    joinerNumber % dispatcherNumber == 0,
    s"joinerNumber must be a multiple of dispatcherNumber",
  )
  require(
    sampleStorageNumPerDispatcher > 0 &&
      sampleStorageNumPerDispatcher <= storageNumber,
    s"sampleStorageNum $sampleStorageNumPerDispatcher must be between 1 and storageNumber $storageNumber",
  )

  val storagePerDispatcher: Int = storageNumber / dispatcherNumber
  val joinerPerDispatcher: Int  = joinerNumber / dispatcherNumber

  val sampledStorages: Set[Int] = {
    (0 until dispatcherNumber)
      .flatMap { dispatcherIdx =>
        if (sampleStorageNumPerDispatcher == 1) { Seq(dispatcherIdx) }
        else {
          (0 until sampleStorageNumPerDispatcher).map { m =>
            val position = (m * (storagePerDispatcher - 1)) /
              (sampleStorageNumPerDispatcher - 1)
            dispatcherIdx + position * dispatcherNumber
          }
        }
      }
      .toSet
  }
  val totalSampleStorageNum: Int = dispatcherNumber *
    sampleStorageNumPerDispatcher

  require(
    sampledStorages.size == totalSampleStorageNum,
    s"Sampled storages calculation failed: expected ${dispatcherNumber *
        sampleStorageNumPerDispatcher} elements, got ${sampledStorages.size}",
  )

  def initClock: TupleClock = {
    if (countBased) { new CountBasedClock() }
    else { RealtimeClock }
  }

  def isSampledStorage(storageIdx: Int): Boolean = {
    sampledStorages.contains(storageIdx)
  }

  def getJoinerIdx(tid: TupleId): Int = {
    tid.dispatcherIdx + (tid.localId % joinerPerDispatcher) * dispatcherNumber
  }

  def joinerIdxToDispatcherIdx(joinerIdx: Int): Int = {
    joinerIdx % dispatcherNumber
  }

  def getStorageIdx(tid: TupleId): Int = {
    tid.dispatcherIdx + (tid.localId % storagePerDispatcher) * dispatcherNumber
  }

  def storageIdxToDispatcherIdx(storageIdx: Int): Int = {
    storageIdx % dispatcherNumber
  }

  def getStorePostion(storageIdx: Int, localId: Int): Int = {
    val base = storageIdx / dispatcherNumber
    val r    = (localId - base) / storagePerDispatcher
    assertEqIfDbg(
      s"storageIdx=$storageIdx, base=$base, localId=$localId, r=$r",
      r * storagePerDispatcher,
      localId - base,
    )
    r
  }

  def adaptNycQ1: Config = {
    val attrNames = ArraySeq
      .from(summon[SpoutTupleStream[NycStream]].attributes.map(_.name))
    this.copy(attrNames = attrNames, maxDistance = 0.99)
  }

  def adaptSpo: Config = {
    val attrNames = ArraySeq
      .from(summon[SpoutTupleStream[SpoStream]].attributes.map(_.name))
    this.copy(attrNames = attrNames, maxDistance = 0.99)
  }

  def adaptBlond: Config = {
    val attrNames = ArraySeq
      .from(summon[SpoutTupleStream[BlondStream]].attributes.map(_.name))
    this.copy(attrNames = attrNames, maxDistance = 0.99)
  }

  def adaptHouseBuyer: Config = adaptHouseBuyer(HouseBuyerQuery())

  def adaptHouseBuyer(query: HouseBuyerQuery): Config = {
    this.copy(
      attrNames = ArraySeq.from(query.attributes.map(_.name)),
      attrQueryNum = query.predicateCount,
      maxDistance = 0.99,
    )
  }

  def adaptTaq: Config = {
    val attrNames = ArraySeq
      .from(summon[SpoutTupleStream[TaqStream]].attributes.map(_.name))
    this.copy(attrNames = attrNames, maxDistance = 1)
  }

  def getMaxSpoutPending: Int = { maxSpoutPending.getOrElse(joinerNumber * 2) }

  def genLocalStormConfig = {
    val cfg = new org.apache.storm.Config()
    cfg.setDebug(false)
    cfg.setNumWorkers(numWorkers)
    cfg.setNumAckers(ackerNumber)
    cfg.put(org.apache.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 300)
    cfg.put(org.apache.storm.Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0)
    cfg.put(
      org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING,
      getMaxSpoutPending,
    )
    cfg
  }

  def genStormConfig = {
    val cfg = genLocalStormConfig
    cfg.put(
      org.apache.storm.Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
      4096,
    );
    cfg
  }

  def ifTrackTime[T](f: => T): T | Null = { if (trackTime) then f else null }
}

object Config {
  def minimal: Config = Config(
    attrQueryNum = 2,
    partitionNum = 2,
    partitionTaskNum = None,
    windowSize = 300,
    bufferCap = 100,
    initialBufferCap = 200,
    maxDistance = 99.0,
    dispatcherNumber = 1,
    storageNumber = 1,
    sampleStorageNumPerDispatcher = 1,
    expireInterval = 10000,
    joinerNumber = 1,
    attrNames = ArraySeq.empty,
    countBased = true,
    numWorkers = 1,
    maxSpoutPending = None,
    trackTime = false,
    trackTimeIntervalMillis = 500,
    emitProb = 0.5,
  )

  def debug: Config = spo2.copy(
    dispatcherNumber = 1,
    windowSize = 30000,
    joinerNumber = 2,
    storageNumber = 2,
    expireInterval = 100,
  )

  def correctness: Config = minimal.copy(
    partitionNum = 4,
    windowSize = 10000,
    bufferCap = 1000,
    initialBufferCap = 2000,
    maxDistance = 0.6,
    dispatcherNumber = 2,
    storageNumber = 4,
    expireInterval = 1000,
    joinerNumber = 4,
  )

  def spoSingle: Config = minimal.copy(
    partitionNum = 1,
    windowSize = 500000,
    bufferCap = 2048,
    initialBufferCap = 4096,
    maxDistance = 1.0,
    dispatcherNumber = 1,
    storageNumber = 2,
    sampleStorageNumPerDispatcher = 1,
    expireInterval = 2000,
    joinerNumber = 5,
    numWorkers = 1,
    maxSpoutPending = Some(50),
  )

  def spo2: Config = minimal.copy(
    partitionNum = 1,
    windowSize = 200000,
    bufferCap = 12000,
    initialBufferCap = 12000,
    maxDistance = 1.0,
    dispatcherNumber = 1,
    storageNumber = 2,
    sampleStorageNumPerDispatcher = 1,
    expireInterval = 1000,
    joinerNumber = 18,
    numWorkers = 2,
    maxSpoutPending = Some(300),
    emitProb = 0.75,
  )

  def spo3: Config = spo2.copy(
    partitionNum = 2,
    storageNumber = 6,
    joinerNumber = 12,
    numWorkers = 3,
    maxSpoutPending = Some(400),
    emitProb = 0.7,
  )

  def spo6: Config = spo2.copy(
    partitionNum = 6,
    windowSize = 1000000,
    bufferCap = 4000,
    storageNumber = 4,
    joinerNumber = 31,
    numWorkers = 6,
    maxSpoutPending = Some(200),
    emitProb = 0.7,
    expireInterval = 3000,
  )

  def blond(machines: Int): Config = {
    machines match {
      case 1 => blond1
      case 2 => blond2
      case 3 => blond3
      case 4 => blond4
      case 5 => blond5
      case 6 => blond6
      case _ => throw new IllegalArgumentException(
          s"blond supports 1 to 6 machines, got $machines",
        )
    }
  }

  def blond1: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 1,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 1,
    joinerNumber = 1,
    maxSpoutPending = Some(200),
  )

  def blond2: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 2,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 2,
    joinerNumber = 10,
    maxSpoutPending = Some(200),
  )

  def blond3: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 3,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 4,
    joinerNumber = 20,
    maxSpoutPending = Some(200),
  )

  def blond4: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 4,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 6,
    joinerNumber = 30,
    maxSpoutPending = Some(400),
  )

  def blond5: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 5,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 8,
    joinerNumber = 40,
    maxSpoutPending = Some(400),
  )

  def blond6: Config = spo6.copy(
    windowSize = 500000,
    numWorkers = 6,
    ackerNumber = 1,
    partitionNum = 1,
    storageNumber = 10,
    joinerNumber = 50,
    maxSpoutPending = Some(400),
  )

  private def houseBuyer6Base: Config = minimal.copy(
    partitionNum = 6,
    windowSize = 1000000,
    bufferCap = 4000,
    initialBufferCap = 12000,
    storageNumber = 4,
    joinerNumber = 31,
    numWorkers = 6,
    maxSpoutPending = Some(200),
    emitProb = 0.7,
    expireInterval = 3000,
  )

  def houseBuyer6: Config = houseBuyer6(HouseBuyerQuery())

  def houseBuyer6(query: HouseBuyerQuery): Config = {
    val partitionsByAttributeCount = Map(
      2  -> List(6, 6),
      4  -> List(6, 6, 6, 6),
      6  -> List(4, 4, 4, 4, 3, 3),
      8  -> List(3, 3, 3, 3, 2, 2, 2, 2),
      10 -> List(2, 2, 2, 2, 2, 2, 2, 2, 1, 1),
    )
    val partitions   = partitionsByAttributeCount(query.attributes.size)
    val partitionNum = partitions.min
    val overrides = query
      .attributes
      .zip(partitions)
      .collect {
        case (attr, count) if count != partitionNum =>
          attr.name -> IndexerCfg(partitionNum = Some(count))
      }
      .toMap
    houseBuyer6Base
      .copy(partitionNum = partitionNum, indexerCfgs = overrides)
      .adaptHouseBuyer(query)
  }

  def taq6: Config = minimal.copy(
    partitionNum = 6,
    windowSize = 1000000,
    bufferCap = 4000,
    initialBufferCap = 12000,
    storageNumber = 4,
    joinerNumber = 31,
    numWorkers = 6,
    maxSpoutPending = Some(200),
    emitProb = 0.5,
    expireInterval = 3000,
  )

  def nyc6: Config = minimal.copy(
    partitionNum = 6,
    windowSize = 1000000,
    bufferCap = 4000,
    initialBufferCap = 12000,
    storageNumber = 4,
    joinerNumber = 31,
    numWorkers = 6,
    maxSpoutPending = Some(200),
    emitProb = 0.5,
    expireInterval = 3000,
  )

  def spoRedblacktree: Config = spo2.copy(
    partitionNum = 6,
    windowSize = 1000000,
    bufferCap = 4000,
    initialBufferCap = 12000,
    storageNumber = 3,
    joinerNumber = 32,
    numWorkers = 6,
    maxSpoutPending = Some(200),
    emitProb = 0.32,
    expireInterval = 12000,
  )

  def partitionBench: Config = minimal.copy(
    partitionNum = 4,
    windowSize = 50000,
    bufferCap = 12000,
    initialBufferCap = 18000,
    dispatcherNumber = 1,
    storageNumber = 1,
    sampleStorageNumPerDispatcher = 1,
    expireInterval = 1000,
    joinerNumber = 2,
    numWorkers = 1,
    maxSpoutPending = Some(300),
    emitProb = 0.4,
  )
}
