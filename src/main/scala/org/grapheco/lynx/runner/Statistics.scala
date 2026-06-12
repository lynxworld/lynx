package org.grapheco.lynx.runner

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}

/**
 * Recording the number of nodes and relationships under various conditions for optimization.
 */
trait Statistics {

  /**
   * Count the number of nodes.
   *
   * @return The number of nodes
   */
  def numNode: Long

  /**
   * Count the number of nodes containing specified label.
   *
   * @param labelName The label to contained
   * @return The number of nodes meeting conditions
   */
  def numNodeByLabel(labelName: LynxNodeLabel): Long

  /**
   * Count the number of nodes containing specified label and property.
   *
   * @param labelName    The label to contained
   * @param propertyName The property to contained
   * @param value        The value of this property
   * @return The number of nodes meeting conditions
   */
  def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long

  /**
   * Count the number of relationships.
   *
   * @return The number of relationships
   */
  def numRelationship: Long

  /**
   * Count the number of relationships have specified type.
   *
   * @param typeName The type
   * @return The number of relationships meeting conditions.
   */
  def numRelationshipByType(typeName: LynxRelationshipType): Long
}
