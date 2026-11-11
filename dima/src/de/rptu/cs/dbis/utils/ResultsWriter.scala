package de.rptu.cs.dbis.utils.resultsWriter

import de.rptu.cs.dbis.domain.tuples.dispatcher.StorageRow
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import java.io.{
  BufferedOutputStream,
  BufferedWriter,
  DataOutputStream,
  FileOutputStream,
}
import de.rptu.cs.dbis.domain.tuples.joiner
import de.rptu.cs.dbis.collection.bitset.Bitset

trait StorageRowExtractor {
  val colNames: List[String]
  val isDefined = !colNames.isEmpty
  def extract(rows: Iterator[StorageRow]): String
}

object EmptyStorageRowExtractor extends StorageRowExtractor {
  val colNames                                     = List()
  def extract(_rows: Iterator[StorageRow]): String = ""
}

/** Binary list of join results next to the CSV: per target tuple its id, the
  * ids of all found tuples and -1 as end marker, each a big-endian long.
  * targetIdx indexes TargetAttrValues.attrValues, foundIdx StorageRow.attrValues.
  */
case class ResultIdsCols(targetIdx: Int, foundIdx: Int)

case class ResultsWriter(
  extraTargetCols: List[(Int, String)] =
    List(), // the index of values in TargetAttrValues.attrValues (-1)
  extraFoundValuesCols: List[(Int, String)] =
    List(), // the index of values in StorageRow.attrValues (-2)
  resultIdsCols: Option[ResultIdsCols] = None,
) extends StrictLogging {
  private val sepChar = ','

  def initIdsFile(p: File): Option[DataOutputStream] = {
    resultIdsCols.map { _ =>
      if (p.exists) p.delete()
      p.parent.createDirectories()
      logger.info(s"writing result ids to $p")
      new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(p.toJava), 1 << 16),
      )
    }
  }

  def initFile(p: File): BufferedWriter = {
    if (p.exists) p.delete()
    p.parent.createDirectories()
    logger.info(s"creating ResultsWriter on file $p")

    val fileWriter = new BufferedWriter(p.newFileWriter(append = true))

    val cols = List(
      "storageIdx",
      "dispatcherIdx",
      "localId",
      "targetTs",
      "outputTime",
    ) ++ extraTargetCols.map(_._2) ++
      (if (extraFoundValuesCols.isEmpty) List("#found")
       else extraFoundValuesCols.map(_._2))

    fileWriter.write(cols.mkString(","))
    fileWriter.flush()
    fileWriter.newLine()
    fileWriter
  }

  def writeRow[BS](
    writer: BufferedWriter,
    idsOut: Option[DataOutputStream],
    initRowStr: String,
    jr: joiner.Results[BS],
    processFoundIdsAll: (StorageRow => Unit) => Unit,
  )(using Bitset[BS]) = {
    val processFoundIds = idsOut.zip(resultIdsCols) match {
      case Some((out, cols)) => {
        out.writeLong(jr.targetAttrValues(cols.targetIdx).asInstanceOf[Long])
        (processRow: StorageRow => Unit) => {
          processFoundIdsAll(r => {
            out.writeLong(r.attrValues(cols.foundIdx).asInstanceOf[Long])
            processRow(r)
          })
          out.writeLong(-1L)
        }
      }
      case None => processFoundIdsAll
    }
    val outTime = System.currentTimeMillis()
    val row     = new StringBuilder(initRowStr)
    row.append(jr.targetTupleId.localId)
    row.append(sepChar)
    row.append(jr.timestamp)
    row.append(sepChar)
    row.append(outTime)
    for ((i, _name) <- extraTargetCols) {
      row.append(sepChar)
      row.append(jr.targetAttrValues(i))
    }
    if (extraFoundValuesCols.isEmpty) {
      var count = 0
      processFoundIds(_ => { count += 1 })
      row.append(sepChar)
      row.append(count)
      writer.write(row.result())
      writer.newLine()
    } else {
      val commonStr = row.result()
      processFoundIds(r => {
        val foundRow = new StringBuilder(commonStr)
        for ((i, _name) <- extraFoundValuesCols) {
          foundRow.append(sepChar)
          foundRow.append(r.attrValues(i))
        }
        writer.write(foundRow.result())
        writer.newLine()
      })
    }
  }
}
