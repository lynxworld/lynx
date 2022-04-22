package org.grapheco.lynx.procedure

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.LynxType

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]

  def call(args: Seq[LynxValue]): LynxValue

  def signature(name: String) = s"$name(${inputs.map(x => Seq(x._1, x._2).mkString(":")).mkString(",")})"

  def checkArgumentsNumber(actualNumber: Int): Boolean = actualNumber == inputs.size

  def checkArgumentsType(actualArgumentsType: Seq[LynxType]): Boolean = {
    actualArgumentsType.size == inputs.size &&
      inputs.map(_._2).zip(actualArgumentsType).forall{ case (except, actual) => except == actual}
  }
}
