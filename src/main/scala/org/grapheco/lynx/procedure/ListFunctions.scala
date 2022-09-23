package org.grapheco.lynx.procedure

import com.sun.tools.doclets.internal.toolkit.util.DocFinder.Input
import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/**
 * @ClassName List functions return lists of things — nodes in a path, and so on.
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class ListFunctions {
  /**
   * Returns a list containing the string representations
   * for all the property names of a node, relationship, or map.
   * @param x A node, relationship, or map
   * @return property names
   */
  @LynxProcedure(name = "keys")
  def keys(x: LynxValue): List[String] = x match {
    case n: LynxNode => n.keys.toList.map(_.value)
    case r: LynxRelationship => r.keys.toList.map(_.value)
    case m: LynxMap => m.value.keys.toList
    case _ => throw ProcedureException("keys() can only used on node, relationship and map.")
  }

  /**
   * Returns a list containing the string representations for all the labels of a node.
   * @param x The node
   * @return labels
   */
  @LynxProcedure(name = "labels")
  def labels(x: LynxNode): Seq[String] = x.labels.map(_.value)

  /**
   * Returns a list containing all the nodes in a path.
   * @param inputs path
   * @return nodes
   */
  @LynxProcedure(name = "nodes")
  def nodes(inputs: LynxList): List[LynxNode] = {
    def fetchNodeFromList(list: LynxList): LynxNode = {
      list.value.filter(item => item.isInstanceOf[LynxNode]).head.asInstanceOf[LynxNode]
    }

    def fetchListFromList(list: LynxList): LynxList = {
      list.value.filter(item => item.isInstanceOf[LynxList]).head.asInstanceOf[LynxList]
    }

    val list = fetchListFromList(inputs)
    if (list.value.nonEmpty) List(fetchNodeFromList(inputs)) ++ nodes(fetchListFromList(inputs))
    else List(fetchNodeFromList(inputs))
  }

  /**
   * Returns a list comprising all integer values within a specified range. TODO
   * @param inputs
   * @return
   */
  @LynxProcedure(name = "range")
  def range(start: LynxInteger, end: LynxInteger): LynxList = range(start, end, LynxInteger(1))

  @LynxProcedure(name = "range")
  def range(start: LynxInteger, end: LynxInteger, step: LynxInteger): LynxList =
    LynxList((start.value to end.value by step.value).toList map LynxInteger)

  /**
   * Returns a list containing all the relationships in a path.
   * @param inputs path
   * @return relationships
   */
  @LynxProcedure(name = "relationships")
  def relationships(inputs: LynxList): List[LynxRelationship] = {
    val list: LynxList = inputs.value.tail.head.asInstanceOf[LynxList]
    list.value.filter(value => value.isInstanceOf[LynxRelationship]).asInstanceOf[List[LynxRelationship]].reverse
  }

  // TODO : reverse() tail()


  /**
   *  reverse() returns a list in which the order of all elements in the original list have been reversed.
   * @param inputs An list.
   * @return  A list containing homogeneous or heterogeneous elements; the types of the elements are determined by the elements within original.
   */
  @LynxProcedure(name = "reverse")
  def reverse(inputs:LynxList):LynxList={
    LynxList(inputs.value.reverse.toList)
  }


  /**
   * tail() returns a list lresult containing all the elements, excluding the first one, from a list list. Syntax: tail(list)
   * @param inputs An list.
   * @return  A list containing heterogeneous elements; the types of the elements are determined by the elements in list.
   */
  @LynxProcedure(name = "tail")
  def tail(inputs:LynxList): LynxList ={
    LynxList(inputs.value.tail)
  }


}
