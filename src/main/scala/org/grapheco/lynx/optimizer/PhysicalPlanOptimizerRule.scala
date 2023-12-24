package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait PhysicalPlanOptimizerRule {
  def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan

  def optimizeBottomUp(node: PhysicalPlan, ops: PartialFunction[PhysicalPlan, PhysicalPlan]*): PhysicalPlan = {
    val childrenOptimized = node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) {
      (optimized, op) =>
        op.lift(optimized).getOrElse(optimized)
    }
  }
}
