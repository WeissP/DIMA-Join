package de.rptu.cs.dbis.component.joiner

import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Encoder, Json}
import scala.collection.mutable
import scala.collection.mutable.Buffer

case class JoinerMetricPayload(
  joinerIdx: Int,
  aggQueueLength: Int,
  partitionResultDur: Long,
  indexerResultDur: Long,
  targetAttrValuesDur: Long,
  tryProcessDur: Long,
  emitDur: Long,
  partitionCount: Int,
  indexerCount: Int,
  dispatcherCount: Int,
  latestPartitionLID: Int,
  latestIndexerLID: Int,
  latestDispatcherLID: Int,
  lidOrder: Buffer[(Int, String, Long)],
  queueStatus: Json,
)

object JoinerMetricPayload {
  def init = {
    JoinerMetricPayload(
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      Buffer(),
      None.asJson,
    )
  }
}
