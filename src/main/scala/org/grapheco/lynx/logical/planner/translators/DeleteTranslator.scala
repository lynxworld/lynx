package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalDelete, LogicalPlan}
import org.opencypher.v9_0.ast.Delete

//////////////////Delete////////////////
case class DeleteTranslator(delete: Delete) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan =
    LogicalDelete(delete.expressions, delete.forced)(in.get)
}
