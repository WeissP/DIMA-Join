package de.rptu.cs.dbis.component.partition

import scala.language.unsafeNulls
import better.files.File
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}

final case class LeafOpsConfig(
  attrName: String,
  dir: String,
  stopLocalId: Int,
  metadata: Map[String, String] = Map.empty,
) extends Serializable

object LeafOpsWriter {
  private val namespace = "de.rptu.cs.dbis.leafops"

  val storeSchema: Schema = SchemaBuilder
    .record("Store")
    .namespace(namespace)
    .fields()
    .requiredInt("id")
    .requiredInt("key")
    .requiredInt("localId")
    .endRecord()

  val querySchema: Schema = SchemaBuilder
    .record("Query")
    .namespace(namespace)
    .fields()
    .requiredInt("id")
    .requiredInt("key")
    .endRecord()

  val expireSchema: Schema = SchemaBuilder
    .record("Expire")
    .namespace(namespace)
    .fields()
    .requiredInt("id")
    .requiredInt("threshold")
    .endRecord()

  def prefix(partitionIdx: Int): String = s"p$partitionIdx"

  def doneFile(dir: String, partitionIdx: Int): File =
    File(dir) / s"${prefix(partitionIdx)}.done"
}

final class LeafOpsWriter(cfg: LeafOpsConfig, partitionIdx: Int) {
  import LeafOpsWriter.*

  private val flushIntervalMs = 1000L
  private val dir             = File(cfg.dir).createDirectories()
  private val filePrefix      = prefix(partitionIdx)

  private def openWriter(
    kind: String,
    schema: Schema,
  ): DataFileWriter[GenericRecord] = {
    val writer =
      new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](schema))
    val meta = cfg.metadata ++ Map(
      "dima.attribute"    -> cfg.attrName,
      "dima.partitionIdx" -> partitionIdx.toString,
      "dima.stopLocalId"  -> cfg.stopLocalId.toString,
    )
    meta.foreach((k, v) => writer.setMeta(k, v))
    writer.create(schema, (dir / s"${filePrefix}_$kind.avro").toJava)
  }

  private val storeWriter  = openWriter("store", storeSchema)
  private val queryWriter  = openWriter("query", querySchema)
  private val expireWriter = openWriter("expire", expireSchema)
  private val storeRecord  = new GenericData.Record(storeSchema)
  private val queryRecord  = new GenericData.Record(querySchema)
  private val expireRecord = new GenericData.Record(expireSchema)

  private var nextId: Int      = 0
  private var maxLocalId: Int  = -1
  private var lastFlush: Long  = System.currentTimeMillis()
  private var recording: Boolean = true

  def isRecording: Boolean = recording

  def store(values: Iterable[(Int, Int)]): Unit = {
    if (recording) {
      var batchMax = -1
      values.foreach { (key, localId) =>
        storeRecord.put("id", nextId)
        storeRecord.put("key", key)
        storeRecord.put("localId", localId)
        storeWriter.append(storeRecord)
        batchMax = math.max(batchMax, localId)
      }
      finishOp(batchMax)
    }
  }

  def query(key: Int, localId: Int): Unit = {
    if (recording) {
      queryRecord.put("id", nextId)
      queryRecord.put("key", key)
      queryWriter.append(queryRecord)
      finishOp(localId)
    }
  }

  def expire(threshold: Int): Unit = {
    if (recording) {
      expireRecord.put("id", nextId)
      expireRecord.put("threshold", threshold)
      expireWriter.append(expireRecord)
      finishOp(-1)
    }
  }

  private def finishOp(localId: Int): Unit = {
    nextId += 1
    maxLocalId = math.max(maxLocalId, localId)
    if (maxLocalId >= cfg.stopLocalId) { close() }
    else {
      val now = System.currentTimeMillis()
      if (now - lastFlush >= flushIntervalMs) {
        flushAll()
        lastFlush = now
      }
    }
  }

  private def flushAll(): Unit = {
    storeWriter.flush()
    queryWriter.flush()
    expireWriter.flush()
  }

  def close(): Unit = {
    if (recording) {
      recording = false
      storeWriter.close()
      queryWriter.close()
      expireWriter.close()
      doneFile(cfg.dir, partitionIdx)
        .write(s"ops=$nextId maxLocalId=$maxLocalId\n")
    }
  }
}
