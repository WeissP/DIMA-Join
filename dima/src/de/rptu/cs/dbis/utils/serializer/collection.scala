package de.rptu.cs.dbis.utils.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.collection.immutable.Map as IMap
import scala.collection.mutable
import scala.collection.mutable.Map as MMap
import scala.language.unsafeNulls
import de.rptu.cs.dbis.domain.Attr

// copied from package io.altoo.serialization.kryo.scala.serializer
class ScalaMutableMapSerializer() extends Serializer[MMap[?, ?]] {
  override def read(
    kryo: Kryo,
    input: Input,
    typ: Class[MMap[?, ?]],
  ): MMap[?, ?] = {
    val len  = input.readInt(true)
    val coll = kryo.newInstance(typ).empty.asInstanceOf[MMap[Any, Any]]
    if (len != 0) {
      var i = 0
      while (i < len) {
        coll(kryo.readClassAndObject(input)) = kryo.readClassAndObject(input)
        i += 1
      }
    }
    coll
  }

  override def write(
    kryo: Kryo,
    output: Output,
    collection: MMap[?, ?],
  ): Unit = {
    val len = collection.size
    output.writeInt(len, true)
    if (len != 0) {
      val it = collection.iterator
      while (it.hasNext) {
        val t = it.next()
        kryo.writeClassAndObject(output, t._1)
        kryo.writeClassAndObject(output, t._2)
      }
    }
  }
}

class ScalaImmutableMapSerializer() extends Serializer[IMap[?, ?]] {

  setImmutable(true)

  override def read(
    kryo: Kryo,
    input: Input,
    typ: Class[IMap[?, ?]],
  ): IMap[?, ?] = {
    val len = input.readInt(true)
    var coll: IMap[Any, Any] = kryo
      .newInstance(typ)
      .asInstanceOf[IMap[Any, Any]]
      .empty

    if (len != 0) {
      var i = 0
      while (i < len) {
        coll += kryo.readClassAndObject(input) -> kryo.readClassAndObject(input)
        i += 1
      }
    }
    coll
  }

  override def write(
    kryo: Kryo,
    output: Output,
    collection: IMap[?, ?],
  ): Unit = {
    val len = collection.size
    output.writeInt(len, true)
    if (len != 0) {
      val it = collection.iterator
      while (it.hasNext) {
        val t = it.next()
        kryo.writeClassAndObject(output, t._1)
        kryo.writeClassAndObject(output, t._2)
      }
    }
  }
}

class ArrayAttrSerializer[T] extends Serializer[Array[Attr[T]]] {
  override def write(kryo: Kryo, output: Output, buf: Array[Attr[T]]): Unit = {
    output.writeInt(buf.size)
    var i = 0
    while (i < buf.size) {
      kryo.writeObject(output, buf(i), AttrSerializer[T])
      i += 1
    }
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[Array[Attr[T]]],
  ): Array[Attr[T]] = {
    val size = input.readInt()
    val arr  = new Array[Attr[T]](size)
    var i    = 0
    while (i < size) {
      arr(i) = kryo.readObject(input, classOf[Attr[T]], AttrSerializer[T])
      i += 1
    }
    arr
  }
}
