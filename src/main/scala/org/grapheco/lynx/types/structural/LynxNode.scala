package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
import org.grapheco.lynx.types.property.LynxNull
import org.opencypher.v9_0.util.symbols.{CTNode, NodeType}

trait LynxNode extends LynxValue with HasProperty {
  val id: LynxId

  def value: LynxNode = this

  def labels: Seq[LynxNodeLabel]

  def lynxType: NodeType = CTNode

  override def compareTo(o: LynxValue): Int = o match {
    case node: LynxNode => id.toLynxInteger.compareTo(node.id.toLynxInteger)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}

object LynxNode {
  def lynxType: NodeType = CTNode
}
