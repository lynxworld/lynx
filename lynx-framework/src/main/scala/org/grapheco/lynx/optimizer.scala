package org.grapheco.lynx

import org.opencypher.v9_0.ast.{AliasedReturnItem}

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode): PPTNode
}

trait PhysicalPlanOptimizerRule {
  def apply(plan: PPTNode): PPTNode

  def optimizeBottomUp(node: PPTNode, ops: PartialFunction[PPTNode, PPTNode]*): PPTNode = {
    val childrenOptimized = node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) {
      (optimized, op) =>
        op.lift(optimized).getOrElse(optimized)
    }
  }
}

class PhysicalPlanOptimizerImpl(runnerContext: CypherRunnerContext) extends PhysicalPlanOptimizer {
  val rules = Seq[PhysicalPlanOptimizerRule](
    RemoveNullProject,
  )

  def optimize(plan: PPTNode): PPTNode = {
    rules.foldLeft(plan)((optimized, rule) => rule.apply(optimized))
  }
}

object RemoveNullProject extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode): PPTNode = optimizeBottomUp(plan,
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