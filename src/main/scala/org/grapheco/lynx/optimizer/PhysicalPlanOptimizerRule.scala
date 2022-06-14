package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.{PPTNode, PhysicalPlannerContext}

trait PhysicalPlanOptimizerRule {
  def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode

  def optimizeBottomUp(node: PPTNode, ops: PartialFunction[PPTNode, PPTNode]*): PPTNode = {
    val childrenOptimized = node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) {
      (optimized, op) =>
        op.lift(optimized).getOrElse(optimized)
    }
  }
}
