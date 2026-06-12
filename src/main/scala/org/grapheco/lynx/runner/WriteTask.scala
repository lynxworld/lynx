package org.grapheco.lynx.runner

import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue

/**
 * Create and delete of nodes and relationships.
 * Write operations on label, type and property.
 */
trait WriteTask {

  /**
   * Create elements, including nodes and relationships.
   *
   * @param nodesInput         A series of nodes to be created. (the name of this node, the value of this node)
   * @param relationshipsInput A series of relationships to be created. (the name of this relationship, the value of this relationship)
   * @param onCreated          A callback function that needs input the created nodes and relationships
   * @tparam T Return type of onCreated
   * @return Return value of onCreated
   */
  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T

  /**
   * Delete relations.
   *
   * @param ids the ids of relations to deleted
   */
  def deleteRelations(ids: Iterator[LynxId]): Unit

  /**
   * Delete nodes. No need to consider whether the node has relationships.
   *
   * @param ids the ids of nodes to deleted
   */
  def deleteNodes(ids: Seq[LynxId]): Unit

  def updateNode(lynxId: LynxId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]): Option[LynxNode]

  def updateRelationShip(lynxId: LynxId, props: Map[LynxPropertyKey, LynxValue]): Option[LynxRelationship]
  /**
   * Set properties of nodes.
   *
   * @param nodeIds              The ids of nodes to modified
   * @param data                 An array of key value pairs that represent the properties and values to be modified
   * @param cleanExistProperties If it is true, properties will be overwritten; otherwise, properties will be modified
   * @return These nodes after modifying the properties
   */
  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, LynxValue)], cleanExistProperties: Boolean = false): Iterator[Option[LynxNode]]

  /**
   * Add labels to nodes.
   *
   * @param nodeIds The ids of nodes to modified
   * @param labels  The labels to added
   * @return These nodes after adding labels
   */
  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]]

  /**
   * Set properties of relationships.
   *
   * @param relationshipIds The ids of relationships to modified
   * @param data            An array of key value pairs that represent the properties and values to be modified
   * @param cleanExistProperties If it is true, properties will be overwritten; otherwise, properties will be modified
   * @return These relationships after modifying the properties
   */
  def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, LynxValue)], cleanExistProperties: Boolean = false): Iterator[Option[LynxRelationship]]

  /**
   * Set type of relationships.
   *
   * @param relationshipIds The ids of relationships to modified
   * @param typeName        The type to set
   * @return These relationships after set the type
   */
  def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]]

  /**
   * Remove properties of nodes.
   *
   * @param nodeIds The ids of nodes to modified
   * @param data    An array of properties to be removed
   * @return These nodes after modifying the properties
   */
  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]]

  /**
   * Remove labels from nodes.
   *
   * @param nodeIds The ids of nodes to modified
   * @param labels  The labels to removed
   * @return These nodes after removing labels
   */
  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]]

  /**
   * Remove properties of relationships.
   *
   * @param relationshipIds The ids of relationships to modified
   * @param data            An array of properties to be removed
   * @return These relationships after modifying the properties
   */
  def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]]

  /**
   * Remove type of relationships.
   *
   * @param relationshipIds The ids of relationships to modified
   * @param typeName        The type to removed
   * @return These relationships after removing the type
   */
  def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]]

  /**
   * Commit write tasks. It is called at the end of the statement.
   *
   * @return Is it successful?
   */
  def commit: Boolean

}
