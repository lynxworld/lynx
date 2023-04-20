package org.grapheco.lynx.runner

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey}

sealed trait PropOp
object EQUAL extends PropOp
object STARTS_WITH extends PropOp
object ENDS_WITH extends PropOp
object CONTAIN  extends PropOp
object REGULAR extends PropOp
/**
 * labels note: the node with both LABEL1 and LABEL2 labels.
 *
 * @param labels     label names
 * @param properties filter property names
 */
case class NodeFilter(labels: Seq[LynxNodeLabel],
                      properties: Map[LynxPropertyKey, LynxValue],
                      propOps: Map[LynxPropertyKey, PropOp] = Map.empty) {
  def matches(node: LynxNode): Boolean = labels.forall(node.labels.contains) &&
    properties.forall { case (propertyName, value) => node.property(propertyName).exists(value.equals) }
}
