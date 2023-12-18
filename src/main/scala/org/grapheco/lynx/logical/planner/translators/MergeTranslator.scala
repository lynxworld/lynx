package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalMerge, LogicalPlan}
import org.opencypher.v9_0.ast.{Match, Merge}

//////////////merge//////////////////////
case class MergeTranslator(m: Merge) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val matchInfo = Match(true, m.pattern, Seq.empty, m.where)(m.position)
    val mergeIn = MatchTranslator(matchInfo).translate(in)
    LogicalMerge(m.pattern, m.actions)(Option(mergeIn))

    //    if (m.actions.nonEmpty) LPTMergeAction(m.actions)(Option(mergeInfo))
    //    else mergeInfo
  }
}
