package org.grapheco.lynx.optimizer

import org.grapheco.lynx.context.PhysicalPlannerContext
import org.grapheco.lynx.physical.PPTNode

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode
}
