package de.rptu.cs.dbis.utils.debug 

import cats.kernel.Eq
import cats.syntax.all.*
import scala.math.Ordering.Implicits.*
import com.typesafe.scalalogging.StrictLogging

def and(s: Boolean*) = { s.forall(identity) }

object BuildEnv extends StrictLogging {
  final val DebugEnv: Boolean = false

  def ifDbg(f: => Unit): Unit = if (DebugEnv) { f }
  def assertIfDbg(msg: => String, bs: => Boolean*): Unit = ifDbg {
    assert(DebugEnv, "Only reachable if DebugEnv is true")
    if (!and(bs*)) {
      val errorMsg = s"Assertion error: $msg"
      logger.error(errorMsg)
      throw RuntimeException(errorMsg)
    }
  }
  def assertIfDbg(bs: => Boolean*): Unit = assertIfDbg("", bs*)
  def assertEqIfDbg[T](want: => T, got: => T)(using Eq[T]): Unit = {
    assertEqIfDbg("", want, got)
  }
  def assertEqIfDbg[T](msg: => String, want: => T, got: => T)(using
    Eq[T],
  ): Unit = ifDbg {
    val w = want
    val g = got
    assertIfDbg(s"$msg, want: ${w}, got: ${g}", w === g)
  }
  def assertNoMsgsIfDbg(msgs: => Seq[String]): Unit = ifDbg {
    val msg = msgs.mkString("\n")
    assertIfDbg(s"Error Messages:\n$msg", msg.isEmpty)
  }
}

extension [A](seq: scala.collection.Seq[A])
  def isSorted(using ord: Ordering[A]): Boolean = {
    seq
      .sliding(2)
      .forall {
        case collection.Seq(a, b) => a <= b
        case _                    => true
      }
  }
