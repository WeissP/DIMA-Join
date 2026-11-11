package de.rptu.cs.dbis.utils.latencyRecorder

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import java.io.BufferedWriter

object LatencyRecorder extends Serializable with StrictLogging {
  private val sepChar = ','

  def initWriter(p: String): BufferedWriter = {
    val path = File(p)
    if (path.exists) { path.delete() }
    path.parent.createDirectories()
    logger.info(s"creating LatencyRecorder on file $path")

    val fileWriter = new BufferedWriter(path.newFileWriter(append = true))
    fileWriter.write(List("id", "timestamp").mkString(sepChar.toString))
    fileWriter.newLine()
    fileWriter.flush()
    fileWriter
  }

  def record(writer: BufferedWriter, id: Any): Unit = {
    val row = new StringBuilder()
    row.append(id)
    row.append(sepChar)
    row.append(System.currentTimeMillis())
    writer.write(row.result())
    writer.newLine()
  }
}
