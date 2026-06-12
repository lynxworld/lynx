package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.LogicalPlan
import org.opencypher.v9_0.ast.Return

///////////////with,return////////////////
case class ReturnTranslator(r: Return) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val Return(distinct, ri, orderBy, skip, limit, excludedNames) = r

    PipedTranslators(
      Seq(
        ReturnItemsTranslator(ri),
        OrderByTranslator(orderBy),
        SkipTranslator(skip),
        LimitTranslator(limit),
        LPTSelectTranslator(ri),
        DistinctTranslator(distinct)
      )).translate(in)
  }
}
