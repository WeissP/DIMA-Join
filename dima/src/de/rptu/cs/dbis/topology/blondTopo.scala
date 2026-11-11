package de.rptu.cs.dbis.topology

import de.rptu.cs.dbis.topology.helper.MetricConfig
import cats.syntax.all.*
import de.rptu.cs.dbis.topology.helper.AttrInfo
import de.rptu.cs.dbis.utils.serializer.registerStormSerialization
import de.rptu.cs.dbis.domain.config.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.IRichSpout
import de.rptu.cs.dbis.utils.curTimestamp
import de.rptu.cs.dbis.domain.tuples.shuffler.blond.BlondStream
import de.rptu.cs.dbis.domain.tuples.shuffler.blond
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.config.Paths
import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.topology.helper.PartitionStructure
import de.rptu.cs.dbis.topology.helper.SetStructure
import better.files.File
import de.rptu.cs.dbis.component.shuffler.BlondAvroSpout
import de.rptu.cs.dbis.component.shuffler.{AckFeedbackEmit, AdaptiveEmitProb}
import de.rptu.cs.dbis.component.shuffler.EmitController
import de.rptu.cs.dbis.component.shuffler.EmitMode
import de.rptu.cs.dbis.component.shuffler.FixedRateEmit

class BlondTopology(
  cfg: Config,
  spoutGen: String => IRichSpout,
  metricConfig: Option[MetricConfig] = None,
  ps: PartitionStructure = PartitionStructure.Skiplist,
  ss: SetStructure = SetStructure.RoaringBitmap,
) extends StrictLogging {
  val topoGenerator = helper.genTopo[Int, BlondStream](
    cfg,
    BlondTopology.attrs,
    metricConfig,
    ps,
    ss,
    BlondStream.resultDir,
  )

  def runLocal(minutes: Long): Unit = {
    val spouts = List(spoutGen("/dev/null"))

    val resultsRootDir = Paths.result / "local" / "blond"
    val stormConfig    = cfg.genLocalStormConfig
    registerStormSerialization(stormConfig)
    val cluster = new LocalCluster()
    logger.info("cluster created")
    val topo = topoGenerator(
      curTimestamp,
      spouts,
      resultsRootDir,
      BlondStream.debugResultsWriter,
    )
    logger.info(s"topology: $topo")
    cluster.submitTopology("BlondTopology", stormConfig, topo)
    logger.info("submitted")
    // Run for a fixed time for demo purposes
    Thread.sleep(minutes * 60 * 1000)
    logger.info("shutting down the cluster")
    cluster.shutdown()
  }

  def submitTopology(): Unit = {
    val ts             = curTimestamp
    val resultsRootDir = File("/app/results")
    val spouts = List(spoutGen(
      (resultsRootDir / "latency" / "dima" / "blond" / s"${ts}.csv")
        .pathAsString,
    ))
    val name = s"BLOND-${ts}"

    logger.info(show"Submitting topology: $name with config $cfg")
    val stormConfig = cfg.genStormConfig
    registerStormSerialization(stormConfig)

    val topo =
      topoGenerator(ts, spouts, resultsRootDir, BlondStream.benchResultsWriter)
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

object BlondTopology {
  def spoutGenerator(
    cfg: Config,
    datasetPath: Option[String] = None,
    emitMode: EmitMode = EmitMode.Adaptive,
    measureRange: Option[(Long, Long)] = None,
  )(latencyPath: String): IRichSpout = {
    def build[E: EmitController](ec: E): IRichSpout = {
      datasetPath match {
        case Some(p) =>
          BlondAvroSpout(ec, cfg.windowSize, p, latencyPath, measureRange)
        case None =>
          throw new RuntimeException("Blond requires an AVRO dataset path")
      }
    }
    emitMode match {
      case EmitMode.Adaptive => build(new AdaptiveEmitProb(cfg.emitProb))
      case EmitMode.AckFeedback => build(new AckFeedbackEmit(cfg.emitProb, cfg.getMaxSpoutPending))
      case EmitMode.FixedRate(rate, fill) => build(FixedRateEmit(rate, fill))
    }
  }

  val attrs = List(
    AttrInfo(blond.StreamR.powerAttr, ScanOperator.Less),
    AttrInfo(blond.StreamS.powerAttr, ScanOperator.Greater),
    AttrInfo(blond.StreamR.coolAttr, ScanOperator.Greater),
    AttrInfo(blond.StreamS.coolAttr, ScanOperator.Less),
  )
}
