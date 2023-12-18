package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalSetClause}
import org.opencypher.v9_0.ast.SetClause

//////////////////Set////////////////
case class SetTranslator(s: SetClause) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan =
    LogicalSetClause(s)(in)
}
