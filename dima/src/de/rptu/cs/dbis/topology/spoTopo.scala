package de.rptu.cs.dbis.topology

import de.rptu.cs.dbis.topology.helper.MetricConfig
import cats.Show
import cats.syntax.all.*
import de.rptu.cs.dbis.topology.helper.AttrInfo
import de.rptu.cs.dbis.utils.serializer.registerStormSerialization
import de.rptu.cs.dbis.utils.resultsWriter.ResultsWriter
import de.rptu.cs.dbis.domain.tuples.*
import de.rptu.cs.dbis.domain.config.Config
import org.apache.storm
import org.apache.storm.LocalCluster
import org.apache.storm.topology.IRichSpout
import de.rptu.cs.dbis.component.shuffler.SpoSpout
import de.rptu.cs.dbis.utils.curTimestamp
import de.rptu.cs.dbis.domain.tuples.shuffler.spo.SpoStream
import de.rptu.cs.dbis.domain.tuples.shuffler.spo
import de.rptu.cs.dbis.domain.ScanOperator
import de.rptu.cs.dbis.config.Paths
import com.typesafe.scalalogging.StrictLogging
import de.rptu.cs.dbis.topology.helper.PartitionStructure
import de.rptu.cs.dbis.topology.helper.SetStructure
import de.rptu.cs.dbis.component.partition.LeafOpsConfig
import better.files.File
import de.rptu.cs.dbis.component.shuffler.SpoAvroSpout
import de.rptu.cs.dbis.component.shuffler.{AckFeedbackEmit, AdaptiveEmitProb}
import de.rptu.cs.dbis.component.shuffler.EmitController
import de.rptu.cs.dbis.component.shuffler.EmitMode
import de.rptu.cs.dbis.component.shuffler.FixedRateEmit

class SpoTopology(
  cfg: Config,
  spoutGen: String => IRichSpout,
  metricConfig: Option[MetricConfig] = None,
  ps: PartitionStructure = PartitionStructure.Skiplist,
  ss: SetStructure = SetStructure.RoaringBitmap,
  leafOpsConfig: Option[LeafOpsConfig] = None,
) extends StrictLogging {
  val topoGenerator = helper.genTopo[Int, SpoStream](
    cfg,
    SpoTopology.attrs,
    metricConfig,
    ps,
    ss,
    SpoStream.resultDir,
    leafOpsConfig,
  )

  def runLocal(minutes: Long): Unit = {
    val deadline = System.currentTimeMillis() + minutes * 60 * 1000
    runLocalUntil(1000)(() => System.currentTimeMillis() >= deadline)
  }

  def runLocalUntil(pollMs: Long)(isDone: () => Boolean): Unit = {
    val ts             = curTimestamp
    val resultsRootDir = Paths.result / "local" / "spo"
    val spouts = List(spoutGen(
      (resultsRootDir / "latency" / s"$ts.csv").pathAsString,
    ))
    val stormConfig    = cfg.genLocalStormConfig
    registerStormSerialization(stormConfig)
    val cluster = new LocalCluster()
    logger.info("cluster created")
    val topo = topoGenerator(
      ts,
      spouts,
      resultsRootDir,
      SpoStream.debugResultsWriter,
    )
    logger.info(s"topology: $topo")
    cluster.submitTopology("SpoTopology", stormConfig, topo)
    logger.info("submitted")
    while (!isDone()) { Thread.sleep(pollMs) }
    logger.info("shutting down the cluster")
    cluster.shutdown()
  }

  def submitTopology(): Unit = {
    val ts             = curTimestamp
    val resultsRootDir = File("/app/results")
    val spouts = List(spoutGen(
      (resultsRootDir / "latency" / "dima" / "spo" / s"${ts}.csv").pathAsString,
    ))
    val name = s"SPO-${ts}"

    logger.info(show"Submitting topology: $name with config $cfg")
    val stormConfig = cfg.genStormConfig
    registerStormSerialization(stormConfig)

    val topo =
      topoGenerator(ts, spouts, resultsRootDir, SpoStream.benchResultsWriter)
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

object SpoTopology {
  def spoutGenerator(
    cfg: Config,
    datasetPath: Option[String] = None,
    emitMode: EmitMode = EmitMode.Adaptive,
  )(latencyPath: String): IRichSpout = {
    def build[E: EmitController](ec: E): IRichSpout = {
      datasetPath match {
        case Some(p) => SpoAvroSpout(ec, cfg.windowSize, p, latencyPath)
        case None    => SpoSpout(ec, cfg.windowSize)
      }
    }
    emitMode match {
      case EmitMode.Adaptive => build(new AdaptiveEmitProb(cfg.emitProb))
      case EmitMode.AckFeedback => build(new AckFeedbackEmit(cfg.emitProb, cfg.getMaxSpoutPending))
      case EmitMode.FixedRate(rate, fill) => build(FixedRateEmit(rate, fill))
    }
  }

  val attrs = List(
    AttrInfo(spo.StreamR.durationAttr, ScanOperator.Greater),
    AttrInfo(spo.StreamS.timeAttr, ScanOperator.Less),
    AttrInfo(spo.StreamR.revenueAttr, ScanOperator.Less),
    AttrInfo(spo.StreamS.costAttr, ScanOperator.Greater),
  )
}
