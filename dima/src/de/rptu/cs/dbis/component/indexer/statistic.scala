package de.rptu.cs.dbis.component.indexer

import com.bigml.histogram.{Histogram, SimpleTarget}
import de.rptu.cs.dbis.domain.config.Config
import java.util.ArrayList
import de.rptu.cs.dbis.domain.histogram.loadImbalance
import com.typesafe.scalalogging.StrictLogging

extension (ja: java.util.ArrayList[java.lang.Double]) {
  def getDouble(idx: Int): Double = { ja.get(idx).nn }
}

class Statistic(
  partitionNum: Int,
  var histogram: Histogram[SimpleTarget],
  var splitPoints: java.util.ArrayList[java.lang.Double],
) extends Serializable with StrictLogging {

  def distance(other: Histogram[SimpleTarget]): Double = {
    val osp = other.uniform(partitionNum).nn
    if (osp.size() < splitPoints.size()) {
      0.0 // get merged into any statistic with more split Points, it should be fine with lastIdx recorded in distributions
    } else if (osp.size() > splitPoints.size()) { Double.MaxValue }
    else { other.loadImbalance(splitPoints) }
  }

  def put(h: Histogram[SimpleTarget]): Unit = {
    val os = histogram.getTotalCount()
    val is = h.getTotalCount()
    histogram.merge(h)
    val ns = histogram.getTotalCount()
    if (histogram.uniform(partitionNum).nn.size() < splitPoints.size()) {
      logger.warn(
        s"split points size get reduced from ${splitPoints
            .size()} to ${histogram.uniform(partitionNum).nn.size()}, Total count: before: $os, insert: $is, now: $ns",
      )
    }
    splitPoints = histogram.uniform(partitionNum).nn
  }
}

object Statistic extends StrictLogging {
  def apply(cfg: Config, histogram: Histogram[SimpleTarget]): Statistic = {
    new Statistic(
      partitionNum = cfg.partitionNum,
      histogram,
      splitPoints = histogram.uniform(cfg.partitionNum).nn,
    )
  }
}
