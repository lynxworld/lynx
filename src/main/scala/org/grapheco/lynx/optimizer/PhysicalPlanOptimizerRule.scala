package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

import scala.language.implicitConversions

trait PhysicalPlanOptimizerRule {
  def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, this.ops: _*)

  implicit def planOps(p: PhysicalPlan): OperablePhysicalPlan = new OperablePhysicalPlan(p)

  def ops: Seq[PartialFunction[PhysicalPlan, PhysicalPlan]] = Seq.empty

  def optimizeBottomUp(node: PhysicalPlan, ops: PartialFunction[PhysicalPlan, PhysicalPlan]*): PhysicalPlan = {
    val childrenOptimized = node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) {
      (optimized, op) =>
        op.lift(optimized).getOrElse(optimized)
    }
  }
}
