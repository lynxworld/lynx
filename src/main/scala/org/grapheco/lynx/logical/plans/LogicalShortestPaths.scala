package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

case class LogicalShortestPaths(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)], single: Boolean, resName: String)
  extends LeafLogicalPlan {

}
