package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{PhysicalPlan, PPTSet}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.ast.SetItem

//////// SET ///////
case class PPTSetClauseTranslator(setItems: Seq[SetItem]) extends PPTNodeTranslator {
  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    PPTSet(setItems)(in.get, ppc)
  }
}
