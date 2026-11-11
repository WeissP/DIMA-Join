package de.rptu.cs.dbis.component.indexer

import cats.Show
import cats.syntax.all.*
import com.bigml.histogram.{Histogram, SimpleTarget}
import de.rptu.cs.dbis.domain.config.Config
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.StrictLogging

private class Distributions(cfg: Config)
    extends Serializable with StrictLogging {
  private val elems     = ArrayBuffer[Statistic]()
  private var latestIdx = -1

  private def find(h: Histogram[SimpleTarget]): Option[Statistic] = {
    if (latestIdx < 0) { return None }

    val dist = elems(latestIdx).distance(h)

    if (dist <= cfg.maxDistance) { Some(elems(latestIdx)) }
    else {
      val n  = h.uniform(cfg.partitionNum).nn.size()
      val tn = elems(latestIdx).splitPoints.size()
      logger.info(
        s"The distance to latest distribution ($latestIdx-th) is ${dist}($n to  $tn), which is larger than threshold ${cfg.maxDistance}",
      )
      elems
        .view
        .zipWithIndex
        .filter((_, idx) => idx != latestIdx)
        .map((s, idx) => (idx, s, s.distance(h)))
        .filter(_._3 <= cfg.maxDistance)
        .minByOption(_._3) match {
        case None => {
          val ps = elems
            .view
            .map(s => (s.distance(h), s.splitPoints.size()))
            .toList
          logger.info(
            s"All distances $ps are larger than threshold ${cfg.maxDistance}, creating a new distribution of size $n",
          )
          None
        }
        case Some((idx, s, dis)) => {
          logger
            .info(show"found distribution of index ${idx} with distance ${dis}")
          latestIdx = idx
          Some(s)
        }
      }
    }
  }

  def lastSplitPoints: Option[java.util.ArrayList[java.lang.Double]] = {
    Option.when(latestIdx >= 0)(elems(latestIdx).splitPoints)
  }

  def put(h: Histogram[SimpleTarget]): java.util.ArrayList[java.lang.Double] = {
    find(h) match {
      case Some(s) => {
        s.put(h)
        s.splitPoints
      }
      case None => {
        val s = Statistic(cfg, h)
        elems.addOne(s)
        val ps = elems.view.map(s => s.splitPoints.size()).toList
        logger.info(s"a new distribution is created: $ps")
        latestIdx = elems.length - 1
        s.splitPoints
      }
    }
  }
}
