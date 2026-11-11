package de.rptu.cs.dbis.utils.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import de.rptu.cs.dbis.domain.TupleId
import de.rptu.cs.dbis.domain.tuples.trigger.PartitionAction
import de.rptu.cs.dbis.domain.Attr
import scala.language.unsafeNulls

class TupleIdSerializer extends Serializer[TupleId] {
  override def write(kryo: Kryo, output: Output, tupleId: TupleId): Unit = {
    output.writeInt(tupleId.dispatcherIdx)
    output.writeInt(tupleId.localId)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[TupleId],
  ): TupleId = {
    val dispatcherIdx = input.readInt()
    val localId       = input.readInt()
    TupleId(dispatcherIdx, localId)
  }
}

class AttrSerializer[T] extends Serializer[Attr[T]] {
  override def write(kryo: Kryo, output: Output, attr: Attr[T]): Unit = {
    kryo.writeObject(output, attr.tupleId, TupleId.serializer)
    kryo.writeClassAndObject(output, attr.value)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[Attr[T]],
  ): Attr[T] = {
    val tupleId = kryo.readObject(input, classOf[TupleId], TupleId.serializer)
    val value = kryo.readClassAndObject(input).asInstanceOf[T]
    Attr(tupleId, value)
  }
}

class PartitionActionSerializer extends Serializer[PartitionAction] {
  override def write(
    kryo: Kryo,
    output: Output,
    action: PartitionAction,
  ): Unit = { output.writeInt(action.ordinal) }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[PartitionAction],
  ): PartitionAction = { PartitionAction.fromOrdinal(input.readInt()) }
}
