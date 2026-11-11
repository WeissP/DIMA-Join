package de.rptu.cs.dbis.utils.mock

import com.bigml.histogram.{Histogram, SimpleTarget}
import de.rptu.cs.dbis.domain.config.Config

val config: Config = Config.minimal.adaptNycQ1

def histogram(values: Double*): Histogram[SimpleTarget] = {
  val h: Histogram[SimpleTarget] = Histogram(128)
  for (v <- values) { h.insert(v) }
  h
}
