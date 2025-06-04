package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.logical.plans.{GraphPatternEdge, GraphPatternNode}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait ConnectPlanner {
  def plan: Seq[PhysicalPlan]
}

case class DefaultConnectPlanner(source: Candidate, target: Candidate, rel: GraphPatternEdge)(implicit val plannerContext: PhysicalPlannerContext) extends ConnectPlanner {
  override def plan: Seq[PhysicalPlan] = Seq()
}
