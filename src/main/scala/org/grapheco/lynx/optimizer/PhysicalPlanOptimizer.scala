package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait PhysicalPlanOptimizer {
  def optimize(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan
}
