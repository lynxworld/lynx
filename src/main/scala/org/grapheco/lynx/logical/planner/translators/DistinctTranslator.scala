package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalDistinct, LogicalPlan}

case class DistinctTranslator(distinct: Boolean) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    if (distinct) {
      LogicalDistinct()(in.get)
    } else {
      in.get
    }
  }
}
