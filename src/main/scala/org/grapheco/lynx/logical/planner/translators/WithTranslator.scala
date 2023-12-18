package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans._
import org.opencypher.v9_0.ast.{ReturnItems, Where, With}

case class WithTranslator(w: With) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    (w, in) match {
      case (With(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit: Option[LogicalLimit], where), None) =>
        LogicalCreateUnit(items)

      case (With(distinct, ri: ReturnItems, orderBy, skip: Option[LogicalSkip], limit: Option[LogicalLimit], where: Option[Where]), Some(sin)) =>
          LogicalWith(ri)(PipedTranslators(
            Seq(
              ReturnItemsTranslator(ri),
              WhereTranslator(where),
              OrderByTranslator(orderBy),
              SkipTranslator(skip),
              LimitTranslator(limit),
              DistinctTranslator(distinct)
            )).translate(in))
    }
  }
}
