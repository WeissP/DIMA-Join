package de.rptu.cs.dbis.domain.tuples.metric

import de.rptu.cs.dbis.domain.tuples.services.*
import io.circe.syntax.*
import io.circe.{Encoder, Json}
import org.apache.storm.tuple.{Fields, Tuple, Values}

final case class MetricTrigger(id: Long, timestamp: Long)

object MetricTrigger {
  given TupleMessage[MetricTrigger] with {
    extension (t: MetricTrigger) {
      override def tupleValues = new Values(t.id, t.timestamp)
    }
  }

  def fromTuple(tuple: Tuple): MetricTrigger = {
    MetricTrigger(tuple.getLong(0).nn, tuple.getLong(1).nn)
  }
}

case object MetricTriggerKind extends TupleKind {
  val streamId: String = "metric-stream"
  val fields           = new Fields("id", "timestamp")
  val direct           = false
}

final case class MetricKind(
  componentId: String,
  streamId: String,
  location: String,
) extends TupleKind {
  val fields: Fields  = new Fields("metricData")
  val direct: Boolean = false
}

object MetricKind {

  /** Creates a map from stream ID to location for a variable number of MetricKind instances.
    *
    * @param kinds A variable number of MetricKind objects.
    * @return A map where keys are stream IDs and values are locations.
    */
  def buildMap(kinds: MetricKind*): Map[String, String] = {
    kinds.map(kind => kind.streamId -> kind.location).toMap
  }
}

opaque type MetricMessage = String

object MetricMessage {

  /** Constructs a MetricMessage by serializing a payload object to JSON and combining it with trigger info.
    *
    * @param trigger The MetricTrigger associated with this message.
    * @param payload The object to serialize into the JSON field.
    * @param encoder Implicit Circe encoder for the payload type A.
    * @tparam A The type of the payload object.
    * @return A new MetricMessage instance.
    */
  def apply[A](trigger: MetricTrigger, payload: A)(using
    encoder: Encoder[A], 
  ): MetricMessage = {
    val payloadJson: Json = payload.asJson
    val combinedJson: Json = Json.obj(
      "id"        -> trigger.id.asJson,
      "timestamp" -> trigger.timestamp.asJson,
      "payload"   -> payloadJson,
    )
    combinedJson.noSpaces // The String is the MetricMessage
  }

  def apply[A](payload: A)(using encoder: Encoder[A]): MetricMessage = {
    payload.asJson.noSpaces
  }

  // TupleMessage instance for the opaque type
  given TupleMessage[MetricMessage] with {
    extension (t: MetricMessage) { // t is the JSON String
      override def tupleValues: Values =
        new Values(t) // Emit the single JSON string
    }
  }

  def fromTuple(tuple: Tuple): MetricMessage = { tuple.getString(0).nn }
}
