package org.grapheco.lynx.types.structural

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.{InvalidValueException, LynxValue, TypeException}
import org.opencypher.v9_0.util.symbols.CTPath

case class LynxPath(elements: Seq[LynxElement]) extends LynxValue{

  override def value: Any = elements

  override def lynxType: LynxType = CTPath

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  def append(rel: LynxRelationship, node: LynxNode): LynxPath = new LynxPath(elements ++ Seq(rel, node))

  def append(eles: Seq[LynxElement]): LynxPath = {
    LynxPath.check(eles.+:(elements.head))
    new LynxPath(elements ++ eles)
  }

}

object LynxPath{
  def apply(elements: Seq[LynxElement]): LynxPath ={
    check(elements)
    new LynxPath(elements)
  }

  def check(elements: Seq[LynxElement]): Boolean = {
    if (elements.length < 3)
      throw InvalidValueException("A path contains at least three elements.")
    if (elements.length % 2 == 0)
      throw InvalidValueException("The number of elements must is an odd.")
    if (elements.headOption.forall(_.isInstanceOf[LynxRelationship]))
      throw InvalidValueException("The first element must is a node.")
    if (elements.lastOption.forall(_.isInstanceOf[LynxRelationship]))
      throw InvalidValueException("The last element must is a node.")
    if (elements.zipWithIndex.forall {
      case (_: LynxNode, i) => i % 2 == 0
      case (_: LynxRelationship, i) => i % 2 == 1
    }) throw InvalidValueException("A path must is an alternating sequence of nodes and relationships.")
    true
  }

  def checkChain(elements: Seq[LynxElement]): Boolean = {
    if(elements.drop(1).dropRight(1).exists(_.isInstanceOf[LynxNode]))
      throw InvalidValueException("A relationship chain must is relationships beside head and last.")
    true
  }
}
