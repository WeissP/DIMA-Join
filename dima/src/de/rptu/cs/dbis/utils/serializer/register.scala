package de.rptu.cs.dbis.utils.serializer

import de.rptu.cs.dbis.domain.*
import de.rptu.cs.dbis.domain.interval.*
import org.apache.storm
import scala.language.unsafeNulls
import de.rptu.cs.dbis.domain.tuples.trigger.PartitionAction

def registerStormSerialization(cfg: storm.Config) = {
  cfg.registerSerialization(classOf[TupleId], classOf[TupleIdSerializer])
  cfg.registerSerialization(classOf[Bound], classOf[BoundSerializer])
  cfg.registerSerialization(classOf[Interval], classOf[IntervalSerializer])
  cfg.registerSerialization(classOf[Attr[?]], classOf[AttrSerializer[?]])
  cfg.registerSerialization(classOf[Option[?]], classOf[OptionSerializer[?]])
  cfg.registerSerialization(classOf[Some[?]], classOf[OptionSerializer[?]])
  cfg.registerSerialization(None.getClass(), classOf[OptionSerializer[?]])

  cfg.registerSerialization(
    classOf[Array[Attr[?]]],
    classOf[ArrayAttrSerializer[?]],
  )
  cfg.registerSerialization(classOf[Array[Object]])

  cfg.registerSerialization(
    classOf[PartitionAction],
    classOf[PartitionActionSerializer],
  )
  cfg.registerSerialization(
    PartitionAction.Release.getClass(),
    classOf[PartitionActionSerializer],
  )
  cfg.registerSerialization(
    PartitionAction.Send.getClass(),
    classOf[PartitionActionSerializer],
  )
}
