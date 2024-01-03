package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Apply, FromArgument, PPTUnwind, PhysicalPlan}

object RemoveApplyRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, {
    case apply: Apply => apply.right match {
      /* Case 1: Combine Unwind, eg:
        Apply
           ╟──[A]
           ╙──[Unwind]
       =====================
        [Unwind]
           ║
          [A]
      */
      case Some(uw:PPTUnwind) => {
        uw.withChildren(apply.left)
      }

      case _ => apply
    }
    /* Case 2: Combine FromArgument, eg:
      Apply
        ╟──[A]
        ╙──[B*]──[FromArgument]
      =====================
      [B*]
       ║
      [A]
     */
//    case apply: Apply => apply.right.get.leaves match {
//      case Seq(FromArgument(variableName)) => apply.right
//    }
  })
}
