package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalLimit, LogicalPlan}
import org.opencypher.v9_0.ast.Limit

case class LimitTranslator(limit: Option[Limit]) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    limit match {
      case None => in.get
      case Some(Limit(expr)) => LogicalLimit(expr)(in.get)
    }
  }
}
