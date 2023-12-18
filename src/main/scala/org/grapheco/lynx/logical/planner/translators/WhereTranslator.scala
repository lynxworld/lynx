package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalFilter, LogicalPlan}
import org.opencypher.v9_0.ast.Where

case class WhereTranslator(where: Option[Where]) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    where match {
      case None => in.get
      case Some(Where(expr)) => LogicalFilter(expr)(in.get)
    }
  }
}
