package de.rptu.cs.dbis.component.metric

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import java.io.{
  BufferedWriter,
  FileOutputStream,
  OutputStreamWriter,
  PrintWriter,
}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.Try
import scala.language.unsafeNulls
import com.typesafe.scalalogging.StrictLogging

/** A Bolt that receives metrics from specified streams and writes them to files.
  * Each stream's metrics are written to a dedicated file within a specified directory.
  * The filename includes the timestamp when the bolt task was initialized.
  * Assumes each incoming tuple has a single field containing a String message.
  *
  * @param streamLocations A map where keys are source stream IDs and values are the directory paths
  *                        where metric files should be created.
  * @param resultsRootDir The base directory under which stream-specific subdirectories will be created.
  */
class MetricWriter(streamLocations: Map[String, String], resultsRootDir: String)
    extends BaseRichBolt with StrictLogging {

  @transient
  private var collector: OutputCollector = _
  @transient
  private var fileWriters: mutable.Map[String, PrintWriter] = _
  @transient
  private lazy val timestampFormatter = DateTimeFormatter
    .ofPattern("yyyyMMdd-HHmmss")

  override def prepare(
    conf: java.util.Map[String, Object],
    context: TopologyContext,
    collector: OutputCollector,
  ): Unit = {
    this.collector = collector
    this.fileWriters = mutable.Map.empty[String, PrintWriter]
    val timestamp = LocalDateTime.now().format(timestampFormatter)
    Try(Files.createDirectories(Paths.get(resultsRootDir))).recover {
      case e: Exception => logger.error(
          s"MetricWriter: Failed to create root results directory '$resultsRootDir'",
          e,
        )
    }

    streamLocations.foreach { case (streamId, subLocation) =>
      Try {
        val dirPath = Paths.get(resultsRootDir, subLocation)
        Files.createDirectories(dirPath)
        val filePath = dirPath.resolve(s"${timestamp}.ndjson")
        val fos      = new FileOutputStream(filePath.toFile, true)
        val osw      = new OutputStreamWriter(fos, StandardCharsets.UTF_8)
        val bw       = new BufferedWriter(osw)
        val pw       = new PrintWriter(bw, true) // Auto-flush enabled
        fileWriters.put(streamId, pw)
      }.recover { case e: Exception =>
        logger.error(
          s"MetricWriter: Failed to initialize writer for stream '$streamId' at $subLocation within $resultsRootDir",
          e,
        )
      }
    }
  }

  override def execute(tuple: Tuple): Unit = {
    val streamId = tuple.getSourceStreamId
    fileWriters.get(streamId) match {
      case Some(writer) => Try {
          val message = tuple.getString(0)
          writer.println(message)
          collector.ack(tuple)
        }.recover { case e: Exception =>
          logger.error(
            s"MetricWriter: Failed to write message from stream '$streamId'. Message: ${Try(tuple.getString(0))
                .getOrElse("[failed to get message]")}",
            e,
          )
          collector.fail(tuple)
        }
      case None =>
        logger.warn(
          s"MetricWriter: Received tuple from unexpected stream '$streamId'. Discarding.",
        )
        collector.ack(tuple)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}

  override def cleanup(): Unit = {
    fileWriters.foreach { case (streamId, writer) =>
      Try(writer.close()).recover { case e: Exception =>
        logger.error(
          s"MetricWriter: Error closing writer for stream '$streamId'",
          e,
        )
      }
    }
    fileWriters.clear()
    logger.info("MetricWriter: Closed file writers.")
  }
}

object MetricWriter {
  val componentId: String = "MetricWriter"
}
