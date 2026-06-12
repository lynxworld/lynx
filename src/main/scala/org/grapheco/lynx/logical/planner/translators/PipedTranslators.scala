package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.LogicalPlan

//pipelines a set of LPTNodes
case class PipedTranslators(items: Seq[LogicalTranslator]) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    items.foldLeft[Option[LogicalPlan]](in) {
      (in, item) => Some(item.translate(in)(plannerContext))
    }.get
  }
}
