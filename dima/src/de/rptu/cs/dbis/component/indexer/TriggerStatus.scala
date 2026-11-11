package de.rptu.cs.dbis.component.indexer

import cats.syntax.all.*
import org.apache.storm.tuple.Tuple

class TriggerStatus extends Serializable {
  private var trigger = none[Tuple]
  private var task    = none[Tuple => Unit]

  def isTransfering: Boolean = task.isDefined
  def transferSucceed: Unit = {
    trigger = None
    task = None
  }

  def setTrigger(tuple: Tuple): Unit = {
    trigger = Some(tuple)
    task.foreach(run => run(tuple))
  }

  def setTask(run: Tuple => Unit): Unit = {
    task = Some(run)
    trigger.foreach(run)
  }
}
