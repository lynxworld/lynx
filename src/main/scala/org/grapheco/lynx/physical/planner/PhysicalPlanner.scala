package org.grapheco.lynx.physical.planner

import org.grapheco.lynx.logical.plans.LogicalPlan
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait PhysicalPlanner {
  def plan(logicalPlan: LogicalPlan)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan
}
