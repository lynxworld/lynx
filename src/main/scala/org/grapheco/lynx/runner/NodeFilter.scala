package org.grapheco.lynx.runner

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey}

/**
 * labels note: the node with both LABEL1 and LABEL2 labels.
 *
 * @param labels     label names
 * @param properties filter property names
 */
case class NodeFilter(labels: Seq[LynxNodeLabel], properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(node: LynxNode): Boolean = labels.forall(node.labels.contains) &&
    properties.forall { case (propertyName, value) => node.property(propertyName).exists(value.equals) }
}
