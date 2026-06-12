package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalOrderBy, LogicalPlan}
import org.opencypher.v9_0.ast.OrderBy

case class OrderByTranslator(orderBy: Option[OrderBy]) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    orderBy match {
      case None => in.get
      case Some(value) => LogicalOrderBy(value.sortItems)(in.get)
    }
  }
}
