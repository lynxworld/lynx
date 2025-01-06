package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Apply, FromArgument, Unwind, PhysicalPlan}

object RemoveApplyRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, {
/* Case 1: Combine Unwind, eg:
        Apply
           ╟──[A]
           ╙──[Unwind]
       =====================
        [Unwind]
           ║
          [A]
*/
    case apply: Apply => 
      // attempt Case 1: Unwind optimization
      val afterUnwindOptimize = apply.right match {
        case Some(uw: Unwind) => uw.withChildren(apply.left)
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

      // 如果 Unwind 优化没有生效，尝试 Case 2: FromArgument 优化
      afterUnwindOptimize match {
        case a: Apply => 
          a.right match {
            case Some(rightPlan) =>
              // 获取右子树的所有叶子节点
              val leaves = collectLeaves(rightPlan)
              // 检查是否只包含 FromArgument
              if (leaves.forall(_.isInstanceOf[FromArgument])) {
                // 获取右子树中除 FromArgument 外的所有操作
                val operations = removeFromArgument(rightPlan)
                operations match {
                  case Some(ops) => ops.withChildren(a.left, None)
                  case None => a.left.getOrElse(a)
                }
              } else a
            case None => a
          }
        case other => other
      }
  })

  // 收集所有叶子节点
  private def collectLeaves(plan: PhysicalPlan): Seq[PhysicalPlan] = {
    if (plan.children.isEmpty) Seq(plan)
    else plan.children.flatMap(p => collectLeaves(p))
  }

  // 移除 FromArgument 并重构查询计划
  private def removeFromArgument(plan: PhysicalPlan): Option[PhysicalPlan] = plan match {
    case _: FromArgument => None
    case p => 
      val newChildren = p.children.flatMap(child => removeFromArgument(child))
      if (newChildren.isEmpty) Some(p.withChildren())
      else Some(p.withChildren(newChildren.map(Some(_)): _*))
  }
}
