package org.grapheco.lynx.types.structural

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.{InvalidValueException, LynxValue, TypeException}
import org.opencypher.v9_0.util.symbols.CTPath

case class LynxPath(elements: Seq[LynxElement]) extends LynxValue {

  override def value: Any = elements

  override def lynxType: LynxType = CTPath

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  def isEmpty: Boolean = elements.isEmpty

  def length: Int = relationships.v.length

  def reversed: LynxPath = new LynxPath(elements.reverse)

  def startNode: Option[LynxNode] = elements.headOption.map(_.asInstanceOf[LynxNode])

  def firstRelationship: Option[LynxRelationship] = elements.lift(1).map(_.asInstanceOf[LynxRelationship])

  def relationships: LynxList = LynxList(elements.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship]).toList)

  def nodes: LynxList = LynxList(elements.filter(_.isInstanceOf[LynxNode]).map(_.asInstanceOf[LynxNode]).toList)

  def trim: LynxPath = new LynxPath(elements.drop(1).dropRight(1))

  def endNode: Option[LynxNode] = elements.lastOption.map(_.asInstanceOf[LynxNode])

  def append(rel: LynxRelationship, node: LynxNode, appendToHead: Boolean = false): LynxPath = {
    if (elements.isEmpty) throw InvalidValueException("The empty path can not append element.")
    val connectedPoint = node.id match {
      case rel.startNodeId => rel.endNodeId
      case rel.endNodeId => rel.startNodeId
      case _ => throw InvalidValueException("The path to append must connected.")
    }
    val connectedPoint_ = (if (appendToHead) startNode else endNode).map(_.id).get
    if (connectedPoint != connectedPoint_) throw InvalidValueException("The path to append must connected.")
    else if (appendToHead) new LynxPath(Seq(node,rel)++elements) else new LynxPath(elements ++ Seq(rel,node))
  }

  def append(lynxPath: LynxPath): LynxPath = {
    if (isEmpty) return lynxPath
    if (lynxPath.isEmpty) return this
    new LynxPath(elements ++ lynxPath.elements)
  }

  def append(lynxElement: LynxElement): LynxPath = {
    new LynxPath(elements :+ lynxElement)
  }

  def connect(lynxPath: LynxPath): LynxPath = {
    if (isEmpty) return lynxPath
    if (lynxPath.isEmpty) return this
    if (endNode.get.id != lynxPath.startNode.get.id) throw InvalidValueException("The path to connect must connected.")
    else new LynxPath(elements ++ lynxPath.elements.drop(1))
  }

  def connectLeft(lynxPath: LynxPath):LynxPath = lynxPath.connect(this)

}

object LynxPath{

  val EMPTY = new LynxPath(Seq.empty)

  def apply(elements: Seq[LynxElement]): LynxPath ={
    check(elements)
    new LynxPath(elements)
  }

  def startPoint(node: LynxNode): LynxPath = {
    new LynxPath(Seq(node))
  }

  def singleRel(relationship: LynxRelationship): LynxPath = {
    new LynxPath(Seq(relationship))
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
    if (!elements.zipWithIndex.forall {
      case (_: LynxNode, i) => i % 2 == 0
      case (_: LynxRelationship, i) => i % 2 == 1
    }) throw InvalidValueException("A path must is an alternating sequence of nodes and relationships.")
    true
  }
}
