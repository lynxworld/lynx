package org.grapheco.lynx.physical.planner

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan

trait PPTNodeTranslator {
  def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan
}
