package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.plans.{PhysicalPlan, Project}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.ast.AliasedReturnItem

/**
 * @ClassName RemoveNullProject
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
object RemoveNullProject extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan,
    {
      case pnode: PhysicalPlan =>
        pnode.children match {
          case Seq(p@Project(ri)) if ri.items.forall {
            case AliasedReturnItem(expression, variable) => expression == variable
          } => pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

          case _ => pnode
        }
    }
  )
}
