package org.grapheco.lynx.physical

import org.grapheco.lynx.context.PhysicalPlannerContext
import org.grapheco.lynx.logical.LPTNode

trait PhysicalPlanner {
  def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode
}
