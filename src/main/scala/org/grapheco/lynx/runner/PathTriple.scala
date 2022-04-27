package org.grapheco.lynx.runner

import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/**
 * A triplet of path.
 *
 * @param startNode      start node
 * @param storedRelation the relation from start-node to end-node
 * @param endNode        end node
 * @param reverse        If true, it means it is in reverse order
 */
case class PathTriple(startNode: LynxNode, storedRelation: LynxRelationship, endNode: LynxNode, reverse: Boolean = false) {
  def revert: PathTriple = PathTriple(endNode, storedRelation, startNode, !reverse)
}
