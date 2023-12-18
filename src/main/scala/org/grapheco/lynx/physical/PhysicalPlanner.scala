package org.grapheco.lynx.physical

import org.grapheco.lynx.logical.plans.LogicalPlan

trait PhysicalPlanner {
  def plan(logicalPlan: LogicalPlan)(implicit plannerContext: PhysicalPlannerContext): PPTNode
}
