package org.grapheco.lynx.runner


import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}

/**
 * @ClassName GraphModelHelper
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class GraphModelHelper(graphModel: GraphModel) {
  /*
    Operations of indexes
   */
  def createIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager.createIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def dropIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager.dropIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def indexes: Array[(String, Set[String])] =
    this.graphModel.indexManager.indexes.map { case Index(label, properties) => (label.value, properties.map(_.value)) }

  /*
    Operations of estimating count.
   */
  def estimateNodeLabel(labelName: String): Long =
    this.graphModel.statistics.numNodeByLabel(LynxNodeLabel(labelName))

  def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Long =
    this.graphModel.statistics.numNodeByProperty(LynxNodeLabel(labelName), LynxPropertyKey(propertyName), LynxValue(value))

  def estimateRelationship(relType: String): Long =
    this.graphModel.statistics.numRelationshipByType(LynxRelationshipType(relType))
}
