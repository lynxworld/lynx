package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalUnwind}
import org.opencypher.v9_0.ast.Unwind

//////////////UNWIND//////////////////
case class UnwindTranslator(u: Unwind) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan =
    LogicalUnwind(u)(in)
}
