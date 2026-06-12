package org.grapheco.lynx.runner

import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
/**
 * An index consists of a label and several properties.
 * eg: Index("Person", Set("name"))
 * @param labelName The label
 * @param properties The properties, Non repeatable.
 */
case class Index(labelName: LynxNodeLabel, properties: Set[LynxPropertyKey])

/**
 * Manage index creation and deletion.
 */
trait IndexManager {

  /**
   * Create Index.
   *
   * @param index The index to create
   */
  def createIndex(index: Index): Unit

  /**
   * Drop Index.
   *
   * @param index The index to drop
   */
  def dropIndex(index: Index): Unit

  /**
   * Get all indexes.
   *
   * @return all indexes
   */
  def indexes: Array[Index]
}
