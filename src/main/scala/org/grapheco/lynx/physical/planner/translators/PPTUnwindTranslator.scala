package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{PhysicalPlan, Unwind}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{Expression, Variable}

/////////UNWIND//////////////
case class PPTUnwindTranslator(expression: Expression, variable: Variable) extends PPTNodeTranslator {
  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    Unwind(expression, variable)(in, ppc)
  }
}
