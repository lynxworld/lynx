package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.plans.{Apply, PPTFilter, PPTProject, PhysicalPlan}

object ApplyRule extends PhysicalPlanOptimizerRule {
  override def ops: Seq[PartialFunction[PhysicalPlan, PhysicalPlan]] = Seq(REMOVE_USELESS_APPLY, APPLY_PUSH_DOWN)
  private val REMOVE_USELESS_APPLY: PartialFunction[PhysicalPlan, PhysicalPlan] = {
    /* rule 1: Remove useless Apply, eg:
    Apply
     ╟──[A]
     ╙──[B]
         ║
        [X]*
         ╙──FromArgument
     ======== Replace to: =======
     [B]
      ║
     [X]*
      ║
     [A]
     */
    case apply: Apply => apply
  }

  private val APPLY_PUSH_DOWN: PartialFunction[PhysicalPlan, PhysicalPlan] = {
    /* rule 2: Push down Apply, eg
    Apply
      ╟──[A]
      ╙──[B]*(use result of apply)
          ║
         [X]
    ======== Replace to: =======
    [B]*(use result of apply)
     ╙──Apply
          ╟──[A]
          ╙──[X]
    */
    case apply@Apply(ri) => {
      val left = apply.left
      var right = apply.right
      val returnItemNames = ri.map(_.name)
      while (right.isDefined && right.get.children.length==1){
        val _right = right.get
        if (_right.schema.map(_._1).exists(returnItemNames.contains)) {
          right = _right.left
        } else {
          val root = apply.right
//          apply.withChildren(left, _right)
        }
      }
      apply
    }
  }
}
