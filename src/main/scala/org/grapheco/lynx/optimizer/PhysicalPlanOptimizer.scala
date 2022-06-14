package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.{PPTNode, PhysicalPlannerContext}

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode
}
