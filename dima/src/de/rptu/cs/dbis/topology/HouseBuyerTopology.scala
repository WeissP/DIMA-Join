package de.rptu.cs.dbis.topology

import cats.syntax.all.*
import de.rptu.cs.dbis.topology.helper.{
  AttrInfo,
  MetricConfig,
  PartitionStructure,
  SetStructure,
}
import de.rptu.cs.dbis.utils.serializer.registerStormSerialization
import de.rptu.cs.dbis.domain.config.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.IRichSpout
import de.rptu.cs.dbis.utils.curTimestamp
import de.rptu.cs.dbis.domain.tuples.shuffler.SpoutTupleStream
import de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer.{
  HouseBuyerQuery,
  HouseBuyerStream,
}
import de.rptu.cs.dbis.config.Paths
import com.typesafe.scalalogging.StrictLogging
import better.files.File
import de.rptu.cs.dbis.component.shuffler.{
  HouseBuyerAvroSpout,
  AckFeedbackEmit,
  AdaptiveEmitProb,
  EmitController,
  EmitMode,
  FixedRateEmit,
}
import scala.collection.immutable.ArraySeq

class HouseBuyerTopology(
  cfg: Config,
  spoutGen: String => IRichSpout,
  metricConfig: Option[MetricConfig] = None,
  ps: PartitionStructure = PartitionStructure.Skiplist,
  ss: SetStructure = SetStructure.RoaringBitmap,
  query: HouseBuyerQuery = HouseBuyerQuery(),
  writeResultIds: Boolean = false,
) extends StrictLogging {
  private given SpoutTupleStream[HouseBuyerStream] = HouseBuyerStream
    .forQuery(query)
  private val queryConfig = cfg.copy(
    attrNames = ArraySeq.from(query.attributes.map(_.name)),
    attrQueryNum = query.predicateCount,
  )
  val topoGenerator = helper.genTopo[Float, HouseBuyerStream](
    queryConfig,
    HouseBuyerTopology.attrsFor(query),
    metricConfig,
    ps,
    ss,
    HouseBuyerStream.resultDir,
  )

  def runLocal(minutes: Long): Unit = {
    val spouts = List(spoutGen("/dev/null"))

    val resultsRootDir = Paths.result / "local" / "housebuyer"
    val stormConfig    = queryConfig.genLocalStormConfig
    registerStormSerialization(stormConfig)
    val cluster = new LocalCluster()
    logger.info("cluster created")
    val topo = topoGenerator(
      curTimestamp,
      spouts,
      resultsRootDir,
      HouseBuyerStream.debugResultsWriter,
    )
    logger.info(s"topology: $topo")
    cluster.submitTopology("HouseBuyerTopology", stormConfig, topo)
    logger.info("submitted")
    Thread.sleep(minutes * 60 * 1000)
    logger.info("shutting down the cluster")
    cluster.shutdown()
  }

  def submitTopology(): Unit = {
    val ts             = curTimestamp
    val resultsRootDir = File("/app/results")
    val spouts = List(spoutGen(
      (resultsRootDir / "latency" / "dima" / "housebuyer" / s"${ts}.csv")
        .pathAsString,
    ))
    val name = s"HOUSEBUYER-${ts}"

    logger.info(show"Submitting topology: $name with config $queryConfig")
    val stormConfig = queryConfig.genStormConfig
    registerStormSerialization(stormConfig)

    val topo = topoGenerator(
      ts,
      spouts,
      resultsRootDir,
      if (writeResultIds) { HouseBuyerStream.resultIdsWriter }
      else { HouseBuyerStream.benchResultsWriter },
    )
    logger.info(s"Topology created: $topo")

    try {
      org.apache.storm.StormSubmitter.submitTopology(name, stormConfig, topo)
      logger.info(s"Successfully submitted topology: $name")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to submit topology: ${e.getMessage}")
        throw e
    }
  }
}

object HouseBuyerTopology {
  def spoutGenerator(
    cfg: Config,
    datasetPath: Option[String] = None,
    emitMode: EmitMode = EmitMode.Adaptive,
  )(latencyPath: String): IRichSpout = {
    def build[E: EmitController](ec: E): IRichSpout = {
      datasetPath match {
        case Some(p) => HouseBuyerAvroSpout(ec, cfg.windowSize, p, latencyPath)
        case None =>
          throw new RuntimeException("HouseBuyer requires an AVRO dataset path")
      }
    }
    emitMode match {
      case EmitMode.Adaptive => build(new AdaptiveEmitProb(cfg.emitProb))
      case EmitMode.AckFeedback => build(new AckFeedbackEmit(cfg.emitProb, cfg.getMaxSpoutPending))
      case EmitMode.FixedRate(rate, fill) => build(FixedRateEmit(rate, fill))
    }
  }

  def attrsFor(query: HouseBuyerQuery): List[AttrInfo] = {
    query
      .predicates
      .flatMap { predicate =>
        List(
          AttrInfo(predicate.houseAttr, predicate.houseScanOp),
          AttrInfo(predicate.buyerAttr, predicate.buyerScanOp),
        )
      }
  }

  val attrs: List[AttrInfo] = attrsFor(HouseBuyerQuery())
}
