package de.rptu.cs.dbis

import de.rptu.cs.dbis.domain.config.{Config, IndexerCfg}
import de.rptu.cs.dbis.domain.tuples.shuffler.housebuyer.HouseBuyerQuery
import de.rptu.cs.dbis.topology.{
  BlondTopology,
  HouseBuyerTopology,
  NycFareDistQ1Topology,
  SpoTopology,
  TaqTopology,
}
import de.rptu.cs.dbis.topology.helper.AttrInfo
import de.rptu.cs.dbis.topology.helper.MetricConfig
import de.rptu.cs.dbis.topology.helper.PartitionStructure
import de.rptu.cs.dbis.topology.helper.SetStructure
import de.rptu.cs.dbis.component.storage.Storage
import de.rptu.cs.dbis.component.joiner.Joiner
import de.rptu.cs.dbis.component.shuffler.EmitMode
import better.files.File

val spoConfig                          = Config.spo6.adaptSpo
def blondConfig(machines: Int): Config = Config.blond(machines).adaptBlond
val taqConfig                          = Config.taq6.adaptTaq
val nycConfig                          = Config.nyc6.adaptNycQ1
def houseBuyerConfig(query: HouseBuyerQuery): Config = Config.houseBuyer6(query)

@main
def submitTopologySpo(args: String*): Unit = {
  val parsedArgs   = TopologyArgs.parse(args)
  val config       = applyOverrides(spoConfig, parsedArgs)
  val metricConfig = buildMetricConfig(SpoTopology.attrs, parsedArgs)
  val spout = SpoTopology
    .spoutGenerator(config, parsedArgs.datasetPath, parsedArgs.emitMode)
  SpoTopology(
    config,
    spout,
    metricConfig,
    parsedArgs.partitionStructure,
    parsedArgs.setStructure,
  ).submitTopology()
}

@main
def submitTopologyBlond(args: String*): Unit = {
  val parsedArgs = TopologyArgs.parse(args)
  val config = applyOverrides(
    blondConfig(parsedArgs.numWorkers.getOrElse(6).min(6)),
    parsedArgs,
  )
  val attrNames = BlondTopology.attrs.map(_.attr.name).toSet
  require(
    parsedArgs.attrPartitions.keySet.subsetOf(attrNames),
    s"--attr-partitions names must be in ${attrNames.mkString(", ")}",
  )
  val metricConfig = buildMetricConfig(BlondTopology.attrs, parsedArgs)
  val spout = BlondTopology.spoutGenerator(
    config,
    parsedArgs.datasetPath,
    parsedArgs.emitMode,
    parsedArgs.measureFrom.zip(parsedArgs.measureTo),
  )
  BlondTopology(
    config,
    spout,
    metricConfig,
    parsedArgs.partitionStructure,
    parsedArgs.setStructure,
  ).submitTopology()
}

@main
def submitTopologyTaq(args: String*): Unit = {
  val parsedArgs   = TopologyArgs.parse(args)
  val config       = applyOverrides(taqConfig, parsedArgs)
  val metricConfig = buildMetricConfig(TaqTopology.attrs, parsedArgs)
  val spout = TaqTopology
    .spoutGenerator(config, parsedArgs.datasetPath, parsedArgs.emitMode)
  new TaqTopology(
    config,
    spout,
    metricConfig,
    parsedArgs.partitionStructure,
    parsedArgs.setStructure,
  ).submitTopology()
}

@main
def submitTopologyNyc(args: String*): Unit = {
  val parsedArgs = TopologyArgs.parse(args)
  val datasetPath = parsedArgs
    .datasetPath
    .getOrElse({ throw new IllegalArgumentException("--dataset is required") })
  val config       = applyOverrides(nycConfig, parsedArgs)
  val metricConfig = buildMetricConfig(NycFareDistQ1Topology.attrs, parsedArgs)
  val spout = NycFareDistQ1Topology
    .spoutGenerator(config, datasetPath, parsedArgs.emitMode)
  NycFareDistQ1Topology(
    config,
    metricConfig,
    parsedArgs.partitionStructure,
    parsedArgs.setStructure,
  ).submitTopology(spout, parsedArgs.resultsRootDir)
}

@main
def submitTopologyHouseBuyer(args: String*): Unit = {
  submitHouseBuyerQuery(args, None)
}

private[dbis] def submitHouseBuyerQuery(
  args: Seq[String],
  selectedQuery: Option[HouseBuyerQuery],
): Unit = {
  val parsedArgs = TopologyArgs.parse(args)
  val query = selectedQuery
    .getOrElse(HouseBuyerQuery(parsedArgs.predicateCount))
  require(
    selectedQuery.isEmpty || !args.contains("--predicate-count") ||
      query.predicateCount == parsedArgs.predicateCount,
    "--predicate-count must match the number of selected predicates",
  )
  val config = applyOverrides(houseBuyerConfig(query), parsedArgs)
  val metricConfig =
    buildMetricConfig(HouseBuyerTopology.attrsFor(query), parsedArgs)
  val spout = HouseBuyerTopology
    .spoutGenerator(config, parsedArgs.datasetPath, parsedArgs.emitMode)
  new HouseBuyerTopology(
    config,
    spout,
    metricConfig,
    parsedArgs.partitionStructure,
    parsedArgs.setStructure,
    query,
    parsedArgs.writeResultIds,
  ).submitTopology()
}

private final case class TopologyArgs(
  partitionStructure: PartitionStructure,
  setStructure: SetStructure,
  trackTime: Boolean,
  withMetric: Boolean,
  datasetPath: Option[String],
  windowSize: Option[Long],
  maxDistance: Option[Double],
  numWorkers: Option[Int],
  partitionNum: Option[Int],
  storageNumber: Option[Int],
  joinerNumber: Option[Int],
  ackerNumber: Option[Int],
  maxSpoutPending: Option[Int],
  bufferCap: Option[Int],
  initialBufferCap: Option[Int],
  attrPartitions: Map[String, Int],
  measureFrom: Option[Long],
  measureTo: Option[Long],
  emitMode: EmitMode,
  resultsRootDir: File,
  predicateCount: Int,
  writeResultIds: Boolean,
)

private object TopologyArgs {
  private val PartitionStructureKey = "partition-structure"
  private val SetStructureKey       = "set-structure"
  private val DatasetKey            = "dataset"
  private val ResultsRootDirKey     = "results-root-dir"
  private val WindowSizeKey         = "window-size"
  private val MaxDistanceKey        = "max-distance"
  private val EmitRateKey           = "emit-rate"
  private val FillMaxTuplesKey      = "fill-max-tuples"
  private val EmitModeKey           = "emit-mode"
  private val NumWorkersKey         = "num-workers"
  private val PartitionNumKey       = "partition-num"
  private val StorageNumberKey      = "storage-number"
  private val JoinerNumberKey       = "joiner-number"
  private val AckerNumberKey        = "acker-number"
  private val MaxSpoutPendingKey    = "max-spout-pending"
  private val BufferCapKey          = "buffer-cap"
  private val InitialBufferCapKey   = "initial-buffer-cap"
  private val AttrPartitionsKey     = "attr-partitions"
  private val MeasureFromKey        = "measure-from"
  private val MeasureToKey          = "measure-to"
  private val TrackTimeKey          = "track-time"
  private val MetricKey             = "metric"
  private val PredicateCountKey     = "predicate-count"
  private val WriteResultIdsKey     = "write-result-ids"
  private val DefaultResultsRootDir = File("/app/results")

  def parse(args: Seq[String]): TopologyArgs = {
    val flags = parseFlags(Map.empty, args.toList)

    TopologyArgs(
      partitionStructure = flags
        .get(PartitionStructureKey)
        .map(PartitionStructure.valueOf)
        .getOrElse(PartitionStructure.Skiplist),
      setStructure = flags
        .get(SetStructureKey)
        .map(SetStructure.valueOf)
        .getOrElse(SetStructure.RoaringBitmap),
      trackTime = flags.contains(TrackTimeKey),
      withMetric = flags.contains(MetricKey),
      datasetPath = flags.get(DatasetKey),
      windowSize = flags.get(WindowSizeKey).map(_.toLong),
      maxDistance = flags.get(MaxDistanceKey).map(_.toDouble),
      numWorkers = flags.get(NumWorkersKey).map(_.toInt),
      partitionNum = flags.get(PartitionNumKey).map(_.toInt),
      storageNumber = flags.get(StorageNumberKey).map(_.toInt),
      joinerNumber = flags.get(JoinerNumberKey).map(_.toInt),
      ackerNumber = flags.get(AckerNumberKey).map(_.toInt),
      maxSpoutPending = flags.get(MaxSpoutPendingKey).map(_.toInt),
      bufferCap = flags.get(BufferCapKey).map(_.toInt),
      initialBufferCap = flags.get(InitialBufferCapKey).map(_.toInt),
      attrPartitions = flags
        .get(AttrPartitionsKey)
        .map(
          _.split(",")
            .nn
            .toSeq
            .map(_.nn.split("=").nn.map(_.nn))
            .map {
              case Array(name, n) => name -> n.toInt
              case other => throw new IllegalArgumentException(
                  s"--attr-partitions expects name=count, got ${other.mkString("=")}",
                )
            }
            .toMap,
        )
        .getOrElse(Map.empty),
      measureFrom = flags.get(MeasureFromKey).map(_.toLong),
      measureTo = flags.get(MeasureToKey).map(_.toLong),
      emitMode = flags
        .get(EmitRateKey)
        .map(r => {
          EmitMode.FixedRate(
            r.toDouble,
            flags.get(FillMaxTuplesKey).map(_.toLong).getOrElse(0L),
          )
        })
        .getOrElse {
          require(
            !flags.contains(FillMaxTuplesKey),
            "--fill-max-tuples requires --emit-rate",
          )
          flags.get(EmitModeKey) match {
            case None | Some("adaptive") => EmitMode.Adaptive
            case Some("ack-feedback")    => EmitMode.AckFeedback
            case Some(other) => throw new IllegalArgumentException(
                s"--emit-mode expects adaptive or ack-feedback, got $other",
              )
          }
        },
      predicateCount = flags.get(PredicateCountKey).map(_.toInt).getOrElse(5),
      writeResultIds = flags.contains(WriteResultIdsKey),
      resultsRootDir = flags
        .get(ResultsRootDirKey)
        .map(File(_))
        .getOrElse(DefaultResultsRootDir),
    )
  }

  private def parseFlags(
    flags: Map[String, String],
    args: List[String],
  ): Map[String, String] = {
    args match {
      case Nil => flags
      case "--partition-structure" :: value :: tail =>
        parseFlags(flags + (PartitionStructureKey -> value), tail)
      case "--set-structure" :: value :: tail =>
        parseFlags(flags + (SetStructureKey -> value), tail)
      case "--dataset" :: value :: tail =>
        parseFlags(flags + (DatasetKey -> value), tail)
      case "--results-root-dir" :: value :: tail =>
        parseFlags(flags + (ResultsRootDirKey -> value), tail)
      case "--window-size" :: value :: tail =>
        parseFlags(flags + (WindowSizeKey -> value), tail)
      case "--max-distance" :: value :: tail =>
        parseFlags(flags + (MaxDistanceKey -> value), tail)
      case "--num-workers" :: value :: tail =>
        parseFlags(flags + (NumWorkersKey -> value), tail)
      case "--partition-num" :: value :: tail =>
        parseFlags(flags + (PartitionNumKey -> value), tail)
      case "--storage-number" :: value :: tail =>
        parseFlags(flags + (StorageNumberKey -> value), tail)
      case "--joiner-number" :: value :: tail =>
        parseFlags(flags + (JoinerNumberKey -> value), tail)
      case "--acker-number" :: value :: tail =>
        parseFlags(flags + (AckerNumberKey -> value), tail)
      case "--max-spout-pending" :: value :: tail =>
        parseFlags(flags + (MaxSpoutPendingKey -> value), tail)
      case "--buffer-cap" :: value :: tail =>
        parseFlags(flags + (BufferCapKey -> value), tail)
      case "--initial-buffer-cap" :: value :: tail =>
        parseFlags(flags + (InitialBufferCapKey -> value), tail)
      case "--attr-partitions" :: value :: tail =>
        parseFlags(flags + (AttrPartitionsKey -> value), tail)
      case "--measure-from" :: value :: tail =>
        parseFlags(flags + (MeasureFromKey -> value), tail)
      case "--measure-to" :: value :: tail =>
        parseFlags(flags + (MeasureToKey -> value), tail)
      case "--emit-rate" :: value :: tail =>
        parseFlags(flags + (EmitRateKey -> value), tail)
      case "--fill-max-tuples" :: value :: tail =>
        parseFlags(flags + (FillMaxTuplesKey -> value), tail)
      case "--emit-mode" :: value :: tail =>
        parseFlags(flags + (EmitModeKey -> value), tail)
      case "--predicate-count" :: value :: tail =>
        parseFlags(flags + (PredicateCountKey -> value), tail)
      case "--track-time" :: tail =>
        parseFlags(flags + (TrackTimeKey -> "true"), tail)
      case "--metric" :: tail => parseFlags(flags + (MetricKey -> "true"), tail)
      case "--write-result-ids" :: tail =>
        parseFlags(flags + (WriteResultIdsKey -> "true"), tail)
      case option :: _ =>
        throw new IllegalArgumentException(s"Unknown option: ${option}")
    }
  }
}

private def applyOverrides(config: Config, args: TopologyArgs): Config = {
  val bufferCap = args.bufferCap.getOrElse(config.bufferCap)
  config.copy(
    windowSize = args.windowSize.getOrElse(config.windowSize),
    maxDistance = args.maxDistance.getOrElse(config.maxDistance),
    numWorkers = args.numWorkers.getOrElse(config.numWorkers),
    partitionNum = args.partitionNum.getOrElse(config.partitionNum),
    storageNumber = args.storageNumber.getOrElse(config.storageNumber),
    joinerNumber = args.joinerNumber.getOrElse(config.joinerNumber),
    ackerNumber = args.ackerNumber.getOrElse(config.ackerNumber),
    maxSpoutPending = args.maxSpoutPending.orElse(config.maxSpoutPending),
    bufferCap = bufferCap,
    initialBufferCap = args
      .initialBufferCap
      .getOrElse(math.max(config.initialBufferCap, bufferCap)),
    indexerCfgs = args
      .attrPartitions
      .foldLeft(config.indexerCfgs) { case (cfgs, (name, n)) =>
        cfgs.updated(
          name,
          cfgs.getOrElse(name, IndexerCfg()).copy(partitionNum = Some(n)),
        )
      },
  )
}

private def buildMetricConfig(
  attrs: Seq[AttrInfo],
  args: TopologyArgs,
): Option[MetricConfig] = {
  Option.when(args.withMetric)(MetricConfig(
    500,
    Seq(Joiner.metricKind, Storage.durationMetricKind) ++
      attrs.map(ai => ai.partitionMetricKind),
    Option.when(args.trackTime)(attrs),
  ))
}
