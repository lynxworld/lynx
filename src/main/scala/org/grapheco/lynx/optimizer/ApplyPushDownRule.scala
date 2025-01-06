package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.plans.{Apply, Filter, Project, PhysicalPlan}
import org.opencypher.v9_0.expressions.Variable

object ApplyPushDownRule extends PhysicalPlanOptimizerRule {
  override def ops: Seq[PartialFunction[PhysicalPlan, PhysicalPlan]] = Seq(APPLY_PUSH_DOWN)
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
    // case apply: Apply => apply
    case apply: Apply if apply.right.isEmpty => apply.left
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
    case apply:Apply =>
      val A = apply.left
      val B = apply.right
      // val returnItemNames = A.get.schema.map(_._1)
      val returnItemNames = A.get.schema.map(_._1).toSet

      while (apply.right.isDefined
//        && apply.right.get.children.length==1
        && extraUsage(apply.right.get).exists(returnItemNames.contains)) {
          apply.pushRightDownLeft
//        val r = apply.right.get
//        apply.right = r.left
//        r.left = Some(apply)
      }
      if (apply.right == B) apply else B.get
  }

  private def extraUsage(p: PhysicalPlan): Seq[String] = p match {
    case _@Filter(expr) => expr.findByAllClass[Variable].map(_.name)
    case _ => Seq.empty
  }
}
