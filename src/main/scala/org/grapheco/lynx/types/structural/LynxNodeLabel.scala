package org.grapheco.lynx.types.structural

import org.opencypher.v9_0.expressions.LabelName
import org.opencypher.v9_0.util.InputPosition

import scala.annotation.tailrec
import scala.language.implicitConversions

/**
 * @ClassName LynxNodeLabel
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxNodeLabel(value: String) {
  override def toString: String = value
  def toNodeLabel: LabelName = LabelName(this.value)(InputPosition.NONE)
}

object LynxNodeLabel {
  implicit def fromString(str: String): LynxNodeLabel = LynxNodeLabel(str)
  implicit def fromNodeLabel(nodeLabel: LabelName): LynxNodeLabel = LynxNodeLabel(nodeLabel.name)
}
