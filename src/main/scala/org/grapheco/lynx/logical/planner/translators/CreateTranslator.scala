package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalCreate, LogicalPlan}
import org.opencypher.v9_0.ast.Create

case class CreateTranslator(c: Create) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    LogicalCreate(c.pattern)(in)
  }
}
