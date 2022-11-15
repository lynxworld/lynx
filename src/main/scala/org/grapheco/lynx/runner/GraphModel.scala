package org.grapheco.lynx.runner

import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

trait GraphModel {

  /**
   * A Statistics object needs to be returned.
   * In the default implementation, those number is obtained through traversal and filtering.
   * You can override the default implementation.
   *
   * @return The Statistics object
   */
  def statistics: Statistics = new Statistics {
    override def numNode: Long = nodes().length

    override def numNodeByLabel(labelName: LynxNodeLabel): Long = nodes(NodeFilter(Seq(labelName), Map.empty)).length

    override def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long =
      nodes(NodeFilter(Seq(labelName), Map(propertyName -> value))).length

    override def numRelationship: Long = relationships().length

    override def numRelationshipByType(typeName: LynxRelationshipType): Long =
      relationships(RelationshipFilter(Seq(typeName), Map.empty)).length
  }

  /**
   * An IndexManager object needs to be returned.
   * In the default implementation, the returned indexes is empty,
   * and the addition and deletion of any index will throw an exception.
   * You need override the default implementation.
   *
   * @return The IndexManager object
   */
  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index creation")

    override def dropIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index dropping")

    override def indexes: Array[Index] = Array.empty
  }

  /**
   * An WriteTask object needs to be returned.
   * There is no default implementation, you must override it.
   *
   * @return The WriteTask object
   */
  def write: WriteTask

  /**
   * Find Node By ID.
   *
   * @return An Option of node.
   */
  def nodeAt(id: LynxId): Option[LynxNode]

  /**
   * All nodes.
   *
   * @return An Iterator of all nodes.
   */
  def nodes(): Iterator[LynxNode]

  /**
   * All nodes with a filter.
   *
   * @param nodeFilter The filter
   * @return An Iterator of all nodes after filter.
   */
  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches)

  /**
   * Return all relationships as PathTriple.
   *
   * @return An Iterator of PathTriple
   */
  def relationships(): Iterator[PathTriple]

  /**
   * Return all relationships as PathTriple with a filter.
   *
   * @param relationshipFilter The filter
   * @return An Iterator of PathTriple after filter
   */
  def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = relationships().filter(f => relationshipFilter.matches(f.storedRelation))


  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T =
    this.write.createElements(nodesInput, relationshipsInput, onCreated)

  def deleteRelations(ids: Iterator[LynxId]): Unit = this.write.deleteRelations(ids)

  def deleteNodes(ids: Seq[LynxId]): Unit = this.write.deleteNodes(ids)

  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(String, Any)], cleanExistProperties: Boolean = false): Iterator[Option[LynxNode]] =
    this.write.setNodesProperties(nodeIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)), cleanExistProperties)

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.setNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(String, Any)]): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsProperties(relationshipIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)))

  def setRelationshipsType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[String]): Iterator[Option[LynxNode]] =
    this.write.removeNodesProperties(nodeIds, data.map(LynxPropertyKey))

  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.removeNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[String]): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsProperties(relationshipIds, data.map(LynxPropertyKey))

  def removeRelationshipType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  /**
   * Delete nodes in a safe way, and handle nodes with relationships in a special way.
   *
   * @param nodesIDs The ids of nodes to deleted
   * @param forced   When some nodes have relationships,
   *                 if it is true, delete any related relationships,
   *                 otherwise throw an exception
   */
  def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = relationships().map(_.storedRelation)
      .filter(rel => ids.contains(rel.startNodeId) || ids.contains(rel.endNodeId))
    if (affectedRelationships.nonEmpty) {
      if (forced)
        this.write.deleteRelations(affectedRelationships.map(_.id))
      else
        throw ConstrainViolatedException(s"deleting referred nodes")
    }
    this.write.deleteNodes(ids.toSeq)
  }

  def commit(): Boolean = this.write.commit

  /**
   * Get the paths that meets the conditions
   *
   * @param startNodeFilter    Filter condition of starting node
   * @param relationshipFilter Filter conditions for relationships
   * @param endNodeFilter      Filter condition of ending node
   * @param direction          Direction of relationships, INCOMING, OUTGOING or BOTH
   * @param upperLimit         Upper limit of relationship length
   * @param lowerLimit         Lower limit of relationship length
   * @return The paths
   */
  def paths(startNodeFilter: NodeFilter,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection,
            upperLimit: Int,
            lowerLimit: Int, startFrom: Int = 0): Iterator[LynxPath] = {
    val originStations = nodes(startNodeFilter)
    originStations.flatMap{ originStation =>
      val firstStop = expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
      val leftSteps = Math.min(upperLimit, 100) - lowerLimit // TODO set a super upperLimit
      firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
    }.filter(_.endNode.forall(endNodeFilter.matches))
  }
//  }(direction match {
//      case BOTH => relationships().flatMap(item =>
//        Seq(item, item.revert))
//      case INCOMING => relationships().map(_.revert)
//      case OUTGOING => relationships()
//    }).filter {
//      case PathTriple(startNode, rel, endNode, _) =>
//        relationshipFilter.matches(rel) && startNodeFilter.matches(startNode) && endNodeFilter.matches(endNode)
//    }
//  }

  /**
   * Take a node as the starting or ending node and expand in a certain direction.
   *
   * @param nodeId    The id of this node
   * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion
   */
  def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter(_.startNode.id == nodeId)
  }

  /**
   * Take a node as the starting or ending node and expand in a certain direction with some filter.
   *
   * @param nodeId             The id of this node
   * @param relationshipFilter conditions for relationships
   * @param endNodeFilter      Filter condition of ending node
   * @param direction          The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion and filter
   */
  def expand(nodeId: LynxId,
             relationshipFilter: RelationshipFilter,
             endNodeFilter: NodeFilter,
             direction: SemanticDirection): Iterator[PathTriple] =
    expand(nodeId, direction).filter { pathTriple =>
      relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(pathTriple.endNode)
    }

  def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] =
    expand(id, direction).filter(t => filter.matches(t.storedRelation))

  def expandNonStop(start: LynxNode, relationshipFilter: RelationshipFilter, direction: SemanticDirection, steps: Int): Iterator[LynxPath] = {
    if (steps <= 0) return Iterator(LynxPath.EMPTY)
//    expand(start.id, relationshipFilter, direction).flatMap{ triple =>
//      expandNonStop(triple.endNode, relationshipFilter, direction, steps - 1).map{_.connectLeft(triple.toLynxPath)}
//    }
    val a = expand(start.id, relationshipFilter, direction)
    a.flatMap { triple =>
      expandNonStop(triple.endNode, relationshipFilter, direction, steps - 1).map {
        _.connectLeft(triple.toLynxPath)
      }
    }
  }

  def extendPath(path: LynxPath, relationshipFilter: RelationshipFilter, direction: SemanticDirection, steps: Int): Iterator[LynxPath] = {
    if (path.isEmpty || steps <= 0 ) return Iterator(path)
    Iterator(path) ++
      expand(path.endNode.get.id, relationshipFilter, direction).map(_.toLynxPath)
      .map(_.connectLeft(path)).flatMap(p => extendPath(p, relationshipFilter, direction, steps - 1))
  }
  /**
   * GraphHelper
   */
  val _helper: GraphModelHelper = GraphModelHelper(this)
}
