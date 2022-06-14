package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.{PPTNode, PPTProject, PhysicalPlannerContext}
import org.opencypher.v9_0.ast.AliasedReturnItem

/**
 * @ClassName RemoveNullProject
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
object RemoveNullProject extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode =>
        pnode.children match {
          case Seq(p@PPTProject(ri)) if ri.items.forall {
            case AliasedReturnItem(expression, variable) => expression == variable
          } => pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

          case _ => pnode
        }
    }
  )
}
