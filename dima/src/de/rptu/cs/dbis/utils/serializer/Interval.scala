package de.rptu.cs.dbis.utils.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import de.rptu.cs.dbis.domain.interval.{Bound, Interval}
import scala.language.unsafeNulls

class BoundSerializer extends Serializer[Bound] {
  override def write(kryo: Kryo, output: Output, bound: Bound): Unit = {
    output.writeDouble(bound.value)
    output.writeBoolean(bound.incl)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[Bound]): Bound = {
    val value = input.readDouble()
    val incl  = input.readBoolean()
    Bound(value, incl)
  }
}

class IntervalSerializer extends Serializer[Interval] {
  override def write(kryo: Kryo, output: Output, interval: Interval): Unit = {
    kryo.writeObject(output, interval.lower)
    kryo.writeObject(output, interval.upper)
    output.close()
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[Interval],
  ): Interval = {
    val lower = kryo.readObject(input, classOf[Option[Bound]])
    val upper = kryo.readObject(input, classOf[Option[Bound]])
    Interval(lower, upper)
  }
}
