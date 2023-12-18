package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalRemove}
import org.opencypher.v9_0.ast.Remove

//////////////REMOVE//////////////////
case class RemoveTranslator(r: Remove) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan =
    LogicalRemove(r)(in)
}
