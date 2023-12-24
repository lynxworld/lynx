package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{PhysicalPlan, PPTRemove}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.ast.RemoveItem

/////////REMOVE//////////////
case class PPTRemoveTranslator(removeItems: Seq[RemoveItem]) extends PPTNodeTranslator {
  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    PPTRemove(removeItems)(in.get, ppc)
  }
}
