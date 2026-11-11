package de.rptu.cs.dbis.topology

import de.rptu.cs.dbis.topology.helper.MetricConfig
import de.rptu.cs.dbis.topology.helper.AttrInfo
import de.rptu.cs.dbis.utils.serializer.registerStormSerialization

import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.config.Config
import org.apache.storm
import org.apache.storm.LocalCluster
import org.apache.storm.topology.IRichSpout
import de.rptu.cs.dbis.component.shuffler.NycFareDistSpout
import de.rptu.cs.dbis.utils.curTimestamp
import de.rptu.cs.dbis.domain.tuples.shuffler.NycStream
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.config.Paths
import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.topology.helper.PartitionStructure
import de.rptu.cs.dbis.topology.helper.SetStructure
import better.files.File
import ch.qos.logback.classic.{Level, LoggerContext}
import de.rptu.cs.dbis.component.shuffler.{AckFeedbackEmit, AdaptiveEmitProb}
import de.rptu.cs.dbis.component.shuffler.EmitController
import de.rptu.cs.dbis.component.shuffler.EmitMode
import de.rptu.cs.dbis.component.shuffler.FixedRateEmit
import org.slf4j.LoggerFactory

class NycFareDistQ1Topology(
  cfg: Config,
  metricConfig: Option[MetricConfig] = None,
  ps: PartitionStructure = PartitionStructure.Skiplist,
  ss: SetStructure = SetStructure.SparseBitSet,
) extends StrictLogging {
  val topoGenerator = helper.genTopo[Double, NycStream](
    cfg,
    NycFareDistQ1Topology.attrs,
    metricConfig,
    ps,
    ss,
    NycStream.resultDir,
  )

  def runLocal(minutes: Long, localSpout: IRichSpout): Unit = {
    LoggerFactory.getILoggerFactory match {
      case context: LoggerContext => context
          .getLogger("de.rptu.cs.dbis.component.indexer")
          .nn
          .setLevel(Level.INFO)
      case _ => logger.warn("Could not set local log level to TRACE")
    }
    logger.info("runLocal")
    val resultsRootDir = Paths.result / "local" / "nyc"
    val stormConfig    = cfg.genLocalStormConfig
    registerStormSerialization(stormConfig)
    val cluster = new LocalCluster()
    logger.info("cluster created")
    val resultsWriter = NycStream.query1ResultsWriter
    val topo = topoGenerator(
      curTimestamp,
      List(localSpout),
      resultsRootDir,
      resultsWriter,
    )
    logger.info(s"topology: $topo")
    cluster.submitTopology("NycFareDistQ1Topology", stormConfig, topo)
    logger.info("submitted")
    // Run for a fixed time for demo purposes
    Thread.sleep(minutes * 60 * 1000)
    logger.info("shutting down the cluster")
    cluster.shutdown()
  }

  def submitTopology(
    spoutGen: String => IRichSpout,
    resultsRootDir: File,
  ): Unit = {
    val ts = curTimestamp
    val spouts = List(spoutGen(
      (resultsRootDir / "latency" / "dima" / "nyc" / s"${ts}.csv").pathAsString,
    ))
    val name = s"NycFareDistQ1-${ts}"

    logger.info(s"Submitting topology: $name with config $cfg")
    val stormConfig = cfg.genStormConfig
    registerStormSerialization(stormConfig)

    val topo = topoGenerator(ts, spouts, resultsRootDir, NycStream.metricWriter)
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

object NycFareDistQ1Topology {
  def spoutGenerator(
    cfg: Config,
    datasetPath: String,
    emitMode: EmitMode = EmitMode.Adaptive,
  )(latencyPath: String): IRichSpout = {
    def build[E: EmitController](ec: E): IRichSpout = {
      NycFareDistSpout(ec, cfg.windowSize, datasetPath, latencyPath, cfg)
    }
    emitMode match {
      case EmitMode.Adaptive => build(new AdaptiveEmitProb(cfg.emitProb))
      case EmitMode.AckFeedback => build(new AckFeedbackEmit(cfg.emitProb, cfg.getMaxSpoutPending))
      case EmitMode.FixedRate(rate, fill) => build(FixedRateEmit(rate, fill))
    }
  }

  val attrs = List(
    AttrInfo(NycStream.fareAttribute, ScanOperator.Greater),
    AttrInfo(NycStream.distanceAttribute, ScanOperator.Less),
  )
}
