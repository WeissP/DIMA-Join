package de.rptu.cs.dbis.domain.tuples.trigger

import de.rptu.cs.dbis.domain.tuples.services.*
import org.apache.storm.tuple.{Fields, Tuple, Values}

enum PartitionAction {
  case Send
  case Release
}

extension (v: PartitionAction)
  def switch = v match
    case PartitionAction.Send    => PartitionAction.Release
    case PartitionAction.Release => PartitionAction.Send

case class PartitionKind(val attrName: String) extends TupleKind {
  val streamId: String = s"partition-$attrName"
  val fields           = new Fields("clock", "action", "attr-name")
  val direct           = false
}

final case class Partition(
  clock: Int,
  action: PartitionAction,
  attrName: String,
) {
  val msgId = ("partition", attrName, clock)
}

object Partition {
  given TupleMessage[Partition] with {
    extension (t: Partition) {
      override def tupleValues = new Values(t.clock, t.action, t.attrName)
    }
  }

  def fromTuple(tuple: Tuple): Partition = {
    Partition(
      tuple.getInteger(0).nn,
      tuple.getValue(1).asInstanceOf[PartitionAction],
      tuple.getString(2).nn,
    )
  }
}

case object StorageCleanKind extends TupleKind {
  val streamId = "storage-clean"
  val fields   = new Fields("clock", "sid")
  val direct   = false
}

final case class StorageClean(clock: Int, storageId: Int)

object StorageClean {
  def fromTuple(tuple: Tuple): StorageClean = {
    StorageClean(tuple.getInteger(0).nn, tuple.getInteger(1).nn)
  }

  given TupleMessage[StorageClean] with {
    extension (t: StorageClean) {
      override def tupleValues = new Values(t.clock, t.storageId)
    }
  }

  given HasTupleKind[StorageClean] with {
    extension (t: StorageClean) override def kind = StorageCleanKind
  }
}
