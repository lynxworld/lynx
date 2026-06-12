package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.{LTNode, LynxValue, NodeType, TypeMismatchException}
import org.grapheco.lynx.types.traits.HasProperty

trait LynxNode extends LynxElement {

  def value: LynxNode = this

  def labels: Seq[LynxNodeLabel]

  def lynxType: NodeType = LTNode

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case node: LynxNode => id.toLynxInteger.compareTo(node.id.toLynxInteger)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
