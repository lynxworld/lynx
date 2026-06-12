package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalSelect}
import org.opencypher.v9_0.ast.ReturnItemsDef

case class LPTSelectTranslator(columns: Seq[(String, Option[String])]) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    LogicalSelect(columns)(in.get)
  }
}

object LPTSelectTranslator {
  def apply(ri: ReturnItemsDef): LPTSelectTranslator = LPTSelectTranslator(ri.items.map(item => item.name -> item.alias.map(_.name)))
}