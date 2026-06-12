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

  def matches(node: LynxNode, ignoreProps: LynxPropertyKey*): Boolean = {
    val newProperties: Map[LynxPropertyKey, LynxValue] = properties -- ignoreProps
    if (propOps.isEmpty) { // Compatible with empty propOps. e.g.PPTRelationshipScan
      labels.forall(node.labels.contains) &&
        newProperties.forall {
          case (propertyName, value: LynxValue) => node.property(propertyName).exists(value.equals)
        }
    }
    else {
      labels.forall(node.labels.contains) &&
        newProperties.forall {
          case (propertyName, value) =>
            if (!propOps.contains(propertyName)) {
              false
            } else {
              val maybeOp: Option[PropOp] = propOps.get(propertyName)
              maybeOp match {
                case Some(EQUAL) => {
                  node.property(propertyName).exists(value.equals)
                }
                case Some(NOT_EQUAL) => {
                  if (node.property(propertyName).exists(value.equals)) false else true
                }
                case Some(IN) => {
                  val lynxValues: List[LynxValue] = value.asInstanceOf[LynxList].value
                  lynxValues.foreach(
                    f => if (node.property(propertyName).get.valueEq(f)) {
                     return true
                    })
                  false
                }
                case Some(LESS_THAN) =>
                  val nodeValue = node.property(propertyName)
                  if (nodeValue.isEmpty) false else nodeValue.get.<(value)
                case Some(LESS_THAN_OR_EQUAL) => {
                  val nodeValue = node.property(propertyName)
                  if (nodeValue.isEmpty) false else nodeValue.get.<=(value)
                }
                case Some(GREATER_THAN) => {
                  val nodeValue = node.property(propertyName)
                  if (nodeValue.isEmpty) false else {
                    val bool = nodeValue.get.>(value)
                    bool
                  }
                }
                case Some(GREATER_THAN_OR_EQUAL) => {
                  val nodeValue = node.property(propertyName)
                  if (nodeValue.isEmpty) false else nodeValue.get.>=(value)
                }
                case Some(CONTAINS) => {
                  val lynxValue: LynxValue = node.property(propertyName).get
                  lynxValue.value match {
                    case lynxStr: String =>
                      value.value match {
                        case str: String =>
                          lynxStr.contains(str)
                        case _ =>
                          false
                      }
                    case _ =>
                      false
                  }
                }
                case _ => throw new scala.Exception("unexpected PropOp")
              }
            }
          case _ => false
        }
    }
  }
}
