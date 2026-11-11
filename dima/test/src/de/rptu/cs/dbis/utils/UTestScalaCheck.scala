package de.rptu.cs.dbis.utils

import org.scalacheck.Prop.forAll
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.Test.PropException
import org.scalacheck.util.Pretty
import org.scalacheck.{Gen, Prop, Shrink, Test}
import utest.*

trait PropCheck[G](using s: Shrink[G], pp1: G => Pretty) {
  protected[this] object UTestReporter extends Test.TestCallback {
    private val prettyParams = Pretty.defaultParams

    override def onTestResult(
      name: String,
      res: org.scalacheck.Test.Result,
    ): Unit = {
      import org.scalacheck.Test.Failed
      val scalaCheckResult = {
        if (res.passed) ""
        else {
          val customMsg = res.status match {
            case Failed(args, _) => { onFailed(args(0).arg.asInstanceOf[G]) }
            case PropException(args, e, labels) => {
              val a = args.lift(0).map(_.arg.asInstanceOf[G])
              onException(a, e)
            }
            case m => s"unknown error"
          }
          s"${Pretty.pretty(res, prettyParams)}\n\n${customMsg}"
        }
      }
      assert(scalaCheckResult.isEmpty)
    }
  }

  val minSuccessfulTests    = 100
  val enableShrink: Boolean = true
  def generate: Gen[G]
  def prop(g: G): Prop
  def onFailed(g: G): String = s"Args: g"
  def onException(l: Option[G], e: Throwable): String = {
    s"""PropException occurred!
       |Message: ${e.getMessage}
       |Args: ${l}
       |Stack trace: ${e.getStackTrace.nn.mkString("\n")}
       """.stripMargin
  }
  def execute: Unit = {
    {
      if (enableShrink) { forAll(generate)(prop)(identity, s, pp1) }
      else { forAllNoShrink(generate)(prop)(identity, pp1) }
    }.check(
      Test
        .Parameters
        .default
        .withMinSuccessfulTests(minSuccessfulTests)
        .withTestCallback(UTestReporter),
    )
  }
}

trait PropCheckByGenSuite[G](using genG: Gen[G])(using Shrink[G])(using
  pp1: G => Pretty,
) extends PropCheck[G] {
  var allResults: Map[G, List[(String, Boolean)]] = Map()

  def checkProp(gs: G): List[(String, Boolean)]
  def msgOnFailed(gs: G): String = {
    getOrGenResult(gs)
      .collect { case (msg, false) => s"* $msg" }
      .mkString("\n\n")
  }

  def getOrGenResult(gs: G): List[(String, Boolean)] = {
    allResults.get(gs) match {
      case Some(res) => res
      case None => {
        val res = checkProp(gs)
        allResults = allResults.updated(gs, res)
        res
      }
    }
  }

  override def generate: Gen[G]        = summon[Gen[G]]
  override def prop(gs: G): Prop       = getOrGenResult(gs).forall(_._2)
  override def onFailed(gs: G): String = msgOnFailed(gs)
}
