package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalAggregation, LogicalCreateUnit, LogicalPlan, LogicalProject}
import org.opencypher.v9_0.ast.ReturnItemsDef

case class ReturnItemsTranslator(ri: ReturnItemsDef) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val newIn = in.getOrElse(LogicalCreateUnit(ri.items))
    if (ri.containsAggregate) {
      val (aggregatingItems, groupingItems) = ri.items.partition(i => i.expression.containsAggregate)
      LogicalAggregation(aggregatingItems, groupingItems)(newIn)
    } else {
      LogicalProject(ri)(newIn)
    }
  }
}
