package de.rptu.cs.dbis.component.shuffler

import scala.util.Random
import com.typesafe.scalalogging.StrictLogging

trait EmitController[E] {
  extension (ec: E) {
    def shouldEmit: Boolean
    def startSliding: Unit

    /** Called by the spout for every acked tuple. */
    def onAck: Unit = {}

    /** Called by the spout for every failed tuple. */
    def onFail: Unit = {}
  }
}

class VarEmitProb(startEmitProb: Float, stepEmitProb: Float, stepDur: Long) {
  private var nextStepTime: Long = -1
  private var currentEmitProb    = startEmitProb

  private def startTiming = {
    nextStepTime = System.currentTimeMillis() + stepDur
  }
  private def hasStarted: Boolean = { nextStepTime > 0 }

  private def getOrUpdateEmitProb: Float = {
    if (hasStarted && System.currentTimeMillis() > nextStepTime) {
      currentEmitProb += stepEmitProb
      nextStepTime += stepDur
    }
    currentEmitProb
  }
}

object VarEmitProb {
  def increasing(startEmitProb: Float): VarEmitProb = VarEmitProb(
    startEmitProb = startEmitProb,
    stepEmitProb = 0.05,
    stepDur = 3 * 60 * 1000,
  )

  given EmitController[VarEmitProb] with {
    extension (vep: VarEmitProb) {
      def shouldEmit: Boolean = { Random.nextFloat() < vep.getOrUpdateEmitProb }
      def startSliding: Unit  = { vep.startTiming }
    }
  }
}

class AdaptiveEmitProb(initialEmitProb: Float)
    extends Serializable with StrictLogging {
  private var windowStartTime: Long         = -1L
  private var currentEmitProb: Float        = initialEmitProb
  private var totalEmitsBeforeSliding: Long = 0L
  private var currentWindowEmits: Long      = 0L
  private var previousWindowTps: Float      = 0
  private val windowDuration: Long          = 60 * 1000 // tumbling window in ms
  private val emitStep: Float               = 1.0
  private val emitDur: Float                = 0.082
  private var isSliding: Boolean            = false

  def tuplePerSecond(emit: Long, time: Long): Float = {
    emit.toFloat * 1000 / time
  }

  def showEmit(emit: Long, time: Long): String = {
    s"${tuplePerSecond(emit, time)} t/s"
  }

  def adjustEmitProb(curEmits: Long, time: Long): Unit = {
    val curTps = tuplePerSecond(curEmits, time)
    val targetTps = {
      if (curTps > previousWindowTps) { curTps + emitStep }
      else { curTps }
    }

    val newProb = 1 / (1 - emitDur + 1000.0f / targetTps)

    if (isSliding) {
      logger.info(
        s"previous: ${previousWindowTps} t/s, current: ${showEmit(curEmits, time)}, target: ${targetTps}. Emit Prob changes from $currentEmitProb to $newProb",
      )
      currentEmitProb = newProb
    } else {
      // emit prob can only decrease before the window starts sliding
      val minProb = math.min(currentEmitProb, newProb)
      logger.info(
        s"total emits: $totalEmitsBeforeSliding, previous: $previousWindowTps t/s, current: ${showEmit(curEmits, time)}. Emit Prob changes from $currentEmitProb to $minProb",
      )
      currentEmitProb = minProb
    }

    previousWindowTps = curTps
    currentWindowEmits = 0L
    windowStartTime = System.currentTimeMillis()
  }

  def tryEmit(): Boolean = {
    val now = System.currentTimeMillis()

    if (windowStartTime < 0) {
      logger
        .info(s"Start emitting tuples with initial Emit Prob $currentEmitProb")
      windowStartTime = now
    }
    val time = now - windowStartTime
    // if new tumbling window
    if (time >= windowDuration) { adjustEmitProb(currentWindowEmits, time) }

    val emit = Random.nextFloat() < currentEmitProb
    if (emit) {
      currentWindowEmits += 1L
      if (!isSliding) { totalEmitsBeforeSliding += 1L }
    }
    emit
  }
}

object AdaptiveEmitProb extends StrictLogging {
  given EmitController[AdaptiveEmitProb] with {
    extension (aep: AdaptiveEmitProb) {
      def shouldEmit: Boolean = aep.tryEmit()
      def startSliding: Unit = {
        aep.isSliding = true
        aep.adjustEmitProb(
          aep.currentWindowEmits,
          System.currentTimeMillis() - aep.windowStartTime,
        )
        logger
          .info(s"Window starts sliding with Emit Prob ${aep.currentEmitProb}")
      }
    }
  }
}

/** Emits tuples on a fixed wall-clock schedule of `tuplesPerMinute`.
  *
  * Storm calls `nextTuple` in a loop: after an emit it calls again
  * immediately, after an empty call the spout wait strategy sleeps (~1 ms with
  * the progressive strategy). Slot `k` is due at `start + k * interval`, so the
  * long-run rate does not depend on how long that sleep or an emit takes.
  *
  * At most one tuple is released per `nextTuple` call. Slots that became due
  * during a sleep are released back to back on the following calls. If the
  * spout falls more than `maxLagNanos` behind (e.g. blocked by
  * `max.spout.pending` or backpressure), the overdue slots are skipped instead
  * of being replayed as a burst, and counted in `skipped`.
  *
  * The first `maxRateTuples` tuples are emitted on every call (max rate,
  * bounded only by `max.spout.pending` and backpressure); the fixed schedule
  * starts with the next tuple. This fills the window quickly while the
  * measured part runs at the fixed rate.
  */
class FixedRateEmit(tuplesPerMinute: Double, maxRateTuples: Long = 0L)
    extends Serializable with StrictLogging {
  require(tuplesPerMinute > 0, "tuplesPerMinute must be positive")
  require(maxRateTuples >= 0, "maxRateTuples must not be negative")

  /** Time between two consecutive slots. */
  private val intervalNanos: Long = (60e9 / tuplesPerMinute).toLong

  /** How often the emitted/skipped counters are logged and reset. */
  private val logIntervalNanos = 60 * 1000 * 1000 * 1000L

  /** Maximum lateness that is caught up; older slots are dropped. Catching up
    * slots missed during the ~1 ms spout sleep keeps high rates exact;
    * dropping older ones avoids replaying a long stall as a burst.
    */
  private val maxLagNanos = 100 * 1000 * 1000L

  /** Due time of the next slot; negative until the first call. */
  private var nextEmitNanos: Long = -1L
  private var logStartNanos: Long = -1L

  /** Tuples emitted in the current log interval. */
  private var emitted: Long = 0L

  /** Slots dropped in the current log interval because the spout lagged. */
  private var skipped: Long = 0L

  /** Tuples emitted at max rate so far. */
  private var maxRateEmitted: Long = 0L

  /** Returns true iff the caller must emit exactly one tuple now. */
  def tryEmit(): Boolean = {
    if (maxRateEmitted < maxRateTuples) { tryEmitMaxRate() }
    else { tryEmitFixedRate() }
  }

  private def tryEmitMaxRate(): Boolean = {
    if (maxRateEmitted == 0L) {
      logger.info(s"Start emitting the first $maxRateTuples tuples at max rate")
    }
    maxRateEmitted += 1L
    true
  }

  private def tryEmitFixedRate(): Boolean = {
    val now = System.nanoTime()
    if (nextEmitNanos < 0) {
      logger.info(
        s"Start emitting at $tuplesPerMinute t/min (one tuple every ${intervalNanos / 1e6} ms)",
      )
      nextEmitNanos = now
      logStartNanos = now
    }
    if (now - logStartNanos >= logIntervalNanos) {
      logger.info(
        s"FixedRateEmit: emitted $emitted tuples, skipped $skipped slots in the last ${(now - logStartNanos) / 1e9} s",
      )
      emitted = 0L
      skipped = 0L
      logStartNanos = now
    }
    if (now < nextEmitNanos) { false }
    else {
      val lateness = now - nextEmitNanos
      if (lateness > maxLagNanos) {
        val lag = (lateness - maxLagNanos) / intervalNanos + 1
        skipped += lag
        nextEmitNanos += lag * intervalNanos
      }
      nextEmitNanos += intervalNanos
      emitted += 1L
      true
    }
  }
}

object FixedRateEmit {
  def apply(tuplesPerMinute: Double, maxRateTuples: Long = 0L): FixedRateEmit =
    new FixedRateEmit(tuplesPerMinute, maxRateTuples)

  given EmitController[FixedRateEmit] with {
    extension (fr: FixedRateEmit) {
      def shouldEmit: Boolean = fr.tryEmit()
      def startSliding: Unit  = {}
    }
  }
}

class AckFeedbackEmit(
  initialEmitProb: Double,
  maxSpoutPending: Int,
  queueLow: Double = 20,
  queueHigh: Double = 60,
  queueLowFraction: Double = 0.1,
  queueHighFraction: Double = 0.3,
  queueHighPendingFraction: Double = 0.5,
  smoothing: Double = 0.3,
  persistIntervals: Int = 3,
  intervalNanos: Long = 2000L * 1000 * 1000,
  baseIntervals: Int = 30,
  increase: Double = 1.1,
  decrease: Double = 0.9,
  drain: Double = 0.5,
  minRate: Double = 1.0,
  emitDurMs: Double = 0.082,
  clock: () => Long = () => System.nanoTime(),
) extends Serializable with StrictLogging {
  require(queueLow < queueHigh, "queueLow must be below queueHigh")
  require(
    queueLowFraction < queueHighFraction,
    "queueLowFraction must be below queueHighFraction",
  )
  require(
    initialEmitProb > 0 && initialEmitProb <= 1,
    "initialEmitProb must be in (0, 1]",
  )
  require(maxSpoutPending > 0, "maxSpoutPending must be positive")

  /** The rate at emit probability 1: one emit after another. */
  private val maxRate: Double = 1000.0 / emitDurMs

  private def probToRate(p: Double): Double = {
    if (p >= 1.0) { maxRate }
    else { math.min(maxRate, 1000.0 / (1.0 / p - 1.0 + emitDurMs)) }
  }

  private def rateToProb(rate: Double): Double = {
    math.min(1.0, 1.0 / (1.0 - emitDurMs + 1000.0 / rate))
  }

  private def setTarget(rate: Double): Unit = {
    targetRate = math.min(maxRate, math.max(minRate, rate))
    emitProb = rateToProb(targetRate)
  }

  private var targetRate: Double = probToRate(initialEmitProb)
  private var emitProb: Double   = rateToProb(targetRate)
  private var emitted: Long      = 0L
  private var acked: Long        = 0L
  private var failed: Long       = 0L

  private var intervalStart: Long         = -1L
  private var lastEvent: Long             = -1L
  private var ackedAtStart: Long          = 0L
  private var failedAtStart: Long         = 0L
  private var inFlightIntegral: Double    = 0.0
  private var adjustments: Long           = 0L
  private var sinceShortQueue: Int        = 0
  private var smoothedQueued: Double      = -1.0
  private var overIntervals: Int          = 0
  private var recentTimes: Vector[Double] = Vector.empty

  def currentEmitProb: Double = emitProb

  def currentTargetRate: Double = targetRate

  def inFlight: Long = emitted - acked - failed

  private def accumulate(now: Long): Unit = {
    if (intervalStart < 0) {
      intervalStart = now
      lastEvent = now
    }
    inFlightIntegral += inFlight.toDouble * (now - lastEvent)
    lastEvent = now
  }

  def tryEmit(): Boolean = {
    val now = clock()
    accumulate(now)
    if (now - intervalStart >= intervalNanos) { adjust(now) }
    val emit = Random.nextDouble() < emitProb
    if (emit) { emitted += 1 }
    emit
  }

  def ack(): Unit = {
    accumulate(clock())
    acked += 1
  }

  def fail(): Unit = {
    accumulate(clock())
    failed += 1
  }

  private def adjust(now: Long): Unit = {
    val seconds     = (now - intervalStart) / 1e9
    val ackRate     = (acked - ackedAtStart) / seconds
    val newFailures = failed - failedAtStart
    val avgInFlight = inFlightIntegral / (now - intervalStart)
    val nearPending = avgInFlight >= 0.9 * maxSpoutPending

    def cut(): Unit = {
      setTarget(math.max(math.min(targetRate, ackRate) * decrease, targetRate / 2))
    }

    if (ackRate > 0 && avgInFlight > 0) {
      recentTimes = (recentTimes :+ avgInFlight / ackRate).takeRight(baseIntervals)
      val baseInFlight = ackRate * recentTimes.min
      val queued       = avgInFlight - baseInFlight
      smoothedQueued =
        if (smoothedQueued < 0) { queued }
        else { smoothing * queued + (1 - smoothing) * smoothedQueued }
      val top  = 0.8 * maxSpoutPending - baseInFlight
      val wide = math.max(queueHigh, queueHighFraction * baseInFlight)
      val high = math.min(math.max(wide, queueHighPendingFraction * maxSpoutPending), top)
      val low  = math.min(math.max(queueLow, queueLowFraction * baseInFlight), high / 3)
      if (queued < (low + high) / 2) { sinceShortQueue = 0 }
      else { sinceShortQueue += 1 }
      if (smoothedQueued > high && queued > high) { overIntervals += 1 }
      else { overIntervals = 0 }

      if (newFailures > 0 || nearPending || overIntervals >= persistIntervals) {
        cut()
        overIntervals = 0
      } else if (sinceShortQueue >= baseIntervals) {
        setTarget(ackRate * drain)
        sinceShortQueue = 0
      } else if (queued < (low + high) / 2) {
        setTarget(math.min(targetRate, ackRate * 10) * increase)
      }

      if (adjustments % 30 == 0) {
        logger.info(
          f"AckFeedbackEmit: ack rate $ackRate%.0f t/s, in flight $avgInFlight%.0f, base $baseInFlight%.0f, queued $queued%.0f (smoothed $smoothedQueued%.0f, band $low%.0f-$high%.0f), failed $newFailures, target rate $targetRate%.0f t/s, emit prob $emitProb%.4f",
        )
      }
    } else if (avgInFlight > 0 || newFailures > 0) {
      setTarget(targetRate / 2)
      logger.info(
        f"AckFeedbackEmit: no acks with $avgInFlight%.0f in flight, target rate halved to $targetRate%.0f t/s",
      )
    } else {
      setTarget(targetRate * increase)
    }

    adjustments += 1
    intervalStart = now
    ackedAtStart = acked
    failedAtStart = failed
    inFlightIntegral = 0.0
  }
}

object AckFeedbackEmit {
  given EmitController[AckFeedbackEmit] with {
    extension (af: AckFeedbackEmit) {
      def shouldEmit: Boolean = af.tryEmit()
      def startSliding: Unit  = {}
      override def onAck: Unit  = af.ack()
      override def onFail: Unit = af.fail()
    }
  }
}

/** Which emit controller a spout is built with, selected via CLI flags. */
enum EmitMode {

  /** [[AdaptiveEmitProb]]: probabilistic, ramps the rate up while sliding. */
  case Adaptive

  /** [[FixedRateEmit]]: exact rate, set by `--emit-rate <tuples/min>`; the
    * first `maxRateTuples` tuples (`--fill-max-tuples`) at max rate.
    */
  case FixedRate(tuplesPerMinute: Double, maxRateTuples: Long = 0L)

  /** [[AckFeedbackEmit]]: probabilistic, steered by the spout's acks
    * (`--emit-mode ack-feedback`).
    */
  case AckFeedback
}
