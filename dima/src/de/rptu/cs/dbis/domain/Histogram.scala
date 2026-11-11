package de.rptu.cs.dbis.domain.histogram

import com.bigml.histogram.{Histogram, SimpleTarget}
import scala.math.abs
import com.typesafe.scalalogging.Logger

private val logger = Logger("de.rptu.cs.dbis.domain.histogram.Histogram")

extension (histogram: Histogram[SimpleTarget]) {
  def loadImbalance(
    splitPoints: java.util.ArrayList[java.lang.Double],
  ): Double = {
    val binNum = (splitPoints.size() + 1).toDouble
    if (binNum == 1) { return 0.0 }

    val totalNum    = histogram.getTotalCount()
    val expectedNum = totalNum / binNum
    var dis         = 0.0
    var prefixSum   = 0.0
    var lastSum     = 0.0
    for (idx <- 0 until splitPoints.size()) {
      val splitPoint = splitPoints.get(idx).nn
      val sum        = histogram.sum(splitPoint)
      prefixSum = prefixSum + sum - lastSum - expectedNum
      lastSum = sum
      dis = dis + abs(prefixSum)
    }
    val maxDis = totalNum * (binNum - 1) / 2 + 0.000001
    if (maxDis < dis) {
      logger.warn(
        s"Calculated distance ($dis) exceeds maximum possible distance ($maxDis) " +
          s"for binNum=$binNum, total count=$totalNum, splitPoints=$splitPoints",
      )
    }

    dis / maxDis
  }
}
