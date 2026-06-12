package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalSkip}
import org.opencypher.v9_0.ast.Skip

case class SkipTranslator(skip: Option[Skip]) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    skip match {
      case None => in.get
      case Some(Skip(expr)) => LogicalSkip(expr)(in.get)
    }
  }
}
