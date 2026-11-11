package de.rptu.cs.dbis.domain.clock

sealed trait TupleClock extends Serializable {
  def now: Long
}

object RealtimeClock extends TupleClock {
  def now: Long = System.currentTimeMillis()
}

class CountBasedClock extends TupleClock {
  private var count = -1L

  def now: Long = {
    count += 1
    count
  }
}
