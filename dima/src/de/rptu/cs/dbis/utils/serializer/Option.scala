package de.rptu.cs.dbis.utils.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.Option

class OptionSerializer[A] extends Serializer[Option[A]] {
  override def write(kryo: Kryo, output: Output, option: Option[A]): Unit = {
    option match {
      case Some(value) =>
        output.writeBoolean(true)
        kryo.writeClassAndObject(output, value)
      case None => output.writeBoolean(false)
    }
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[Option[A]],
  ): Option[A] = {
    if (input.readBoolean()) {
      Some(kryo.readClassAndObject(input).asInstanceOf[A])
    } else { None }
  }
}
