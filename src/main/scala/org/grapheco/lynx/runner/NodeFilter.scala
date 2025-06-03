package org.grapheco.lynx.runner

import org.grapheco.lynx.runner.filter.FilterExpr
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxInteger, LynxNull}
import org.grapheco.lynx.types.property.LynxBoolean.TRUE
import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey}


sealed trait PropOp

object EQUAL extends PropOp

object NOT_EQUAL extends PropOp

object LESS_THAN extends PropOp

object LESS_THAN_OR_EQUAL extends PropOp

object GREATER_THAN extends PropOp

object GREATER_THAN_OR_EQUAL extends PropOp

object CONTAINS extends PropOp

object STARTS_WITH extends PropOp

object ENDS_WITH extends PropOp

object CONTAIN extends PropOp

object REGULAR extends PropOp

object IN extends PropOp

/**
 * labels note: the node with both LABEL1 and LABEL2 labels.
 *
 * @param labels     lynx node with label
 * @param properties map contains LynxPropertyKey and LynxValue
 * @param propOps    map contains LynxPropertyKey and PropOp
 */
case class NodeFilter(labels: Seq[LynxNodeLabel],
                      properties: Map[LynxPropertyKey, LynxValue],
                      filterExpr: Option[FilterExpr]) {
  //  Properties will only be set when there is the _lynx_sys_id attribute in Ands
  def matches(node: LynxNode): Boolean = {
    filterExpr match {
      case None =>  labels.forall(node.labels.contains)
      case _ => labels.forall(node.labels.contains) &&
        filterExpr.get.eval((node.keys.map(key => (key, node.property(key).getOrElse(LynxNull)))++Seq((LynxPropertyKey("_lynx_sys_id"), node.id.toLynxInteger))).toMap)
    }
  }
}
