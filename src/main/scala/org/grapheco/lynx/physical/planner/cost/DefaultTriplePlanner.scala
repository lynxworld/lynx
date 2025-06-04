package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.logical.plans.{GraphPatternEdge, GraphPatternNode}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Filter, PhysicalPlan, RelationshipsPlanFactory}

trait TriplePlanner {
  def plan: Seq[PhysicalPlan]
}

case class DefaultTriplePlanner(source: GraphPatternNode,
                                rel: GraphPatternEdge,
                                target: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends TriplePlanner {

  override def plan: Seq[PhysicalPlan] = {
    val factory = RelationshipsPlanFactory(source.variableName, rel.variableName, target.variableName)

    (rel.types.headOption match {
      case Some(relType) => Seq(factory.relByType(relType))
      case None => Seq(factory.allRelationships)
    }).map(_ ~> Filter.multi(rel.expressions))
      .map(_ ~> DefaultNodePlanner(source).makeFilter)
      .map(_ ~> DefaultNodePlanner(target).makeFilter)
  }
}
