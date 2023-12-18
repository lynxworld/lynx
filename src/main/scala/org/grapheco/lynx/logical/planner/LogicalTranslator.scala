package org.grapheco.lynx.logical.planner

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.plans.LogicalPlan

//translates an ASTNode into a LPTNode, `in` as input operator
trait LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan
}
