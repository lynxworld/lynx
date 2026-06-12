package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait PhysicalPlanOptimizer {
  def optimize(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan
}

object PhysicalPlanOptimizer {
  def none: PhysicalPlanOptimizer = new PhysicalPlanOptimizer {
    override def optimize(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = plan
  }
}
