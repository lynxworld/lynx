package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.logical.plans.LogicalShortestPaths
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{PhysicalPlan, ShortestPath}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

case class LPTShortestPathTranslator(lPTShortestPaths: LogicalShortestPaths)(implicit val plannerContext: PhysicalPlannerContext) extends PPTNodeTranslator {
  private def planShortestPaths(paths: LogicalShortestPaths)(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    val LogicalShortestPaths(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)], single, resName) = paths
    chain.toList match {
      //match (m)-[r]-(n)
      case List(Tuple2(rel, rightNode)) => ShortestPath(rel, headNode, rightNode, single, resName)(ppc)
    }
  }

  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    val node = planShortestPaths(lPTShortestPaths)(ppc)
    node
  }
}
