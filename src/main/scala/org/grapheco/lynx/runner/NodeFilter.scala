package org.grapheco.lynx.runner

import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxBoolean
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
                      propOps: Map[LynxPropertyKey, PropOp] = Map.empty) {

  // Helper function to check property matching based on the operation
  private def matchesProperty(node: LynxNode, propertyName: LynxPropertyKey, value: LynxValue, op: PropOp): Boolean = {
    node.property(propertyName) match {
      case Some(nodeValue) =>
        op match {
          case EQUAL => nodeValue.equals(value)
          case NOT_EQUAL => !nodeValue.equals(value)
          case LESS_THAN => nodeValue.<(value)
          case LESS_THAN_OR_EQUAL => nodeValue.<=(value)
          case GREATER_THAN => nodeValue.>(value)
          case GREATER_THAN_OR_EQUAL => nodeValue.>=(value)
          case CONTAINS => nodeValue.value match {
            case lynxStr: String => value.value match {
              case str: String => lynxStr.contains(str)
              case _ => false
            }
            case _ => false
          }
          case IN => value match {
            case lynxList: LynxList => lynxList.value.exists(v => nodeValue.valueEq(v))
            case _ => false
          }
          case _ => throw new scala.Exception(s"Unsupported PropOp: $op")
        }
      case None => false
    }
  }

  // Core matching logic
  def matches(node: LynxNode, ignoreProps: LynxPropertyKey*): Boolean = {
    val newProperties = properties -- ignoreProps
    val hasMatchingLabels = labels.forall(node.labels.contains)
    
    // If no specific property operations, just check labels and simple equality for properties
    if (propOps.isEmpty) {
      hasMatchingLabels && newProperties.forall {
        case (propertyName, value) => node.property(propertyName).contains(value)
      }
    } else {
      hasMatchingLabels && newProperties.forall {
        case (propertyName, value) =>
          propOps.get(propertyName).exists(matchesProperty(node, propertyName, value, _))
      }
    }
  }
}
