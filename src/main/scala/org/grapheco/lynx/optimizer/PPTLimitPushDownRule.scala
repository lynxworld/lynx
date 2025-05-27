package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Limit, OrderBy, PhysicalPlan, RelationshipScan, Skip}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/2/10 11:08
 * @Modified By:
 */
object PPTLimitPushDownRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, {
    case limit: Limit =>{
      val res = pptLimitPushDownRule(limit, ppc)
      if (res._2) res._1.head
      else limit
    }
  })

  private def pptLimitPushDownRule(pf: Limit, ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    pf.children match {
      case Seq(pns@OrderBy(sortItem,expr,skip)) =>
        (Seq(OrderBy(sortItem, pf.expr, skip)(pf.children.head.children.head, ppc)),true)
      case Seq(pns@Skip(expr)) => pns.children match {
        case Seq(order@OrderBy(sortItem,expr,skip)) =>
          (Seq(OrderBy(sortItem, pf.expr, pns.expr)(pf.children.head.children.head.children.head, ppc)),true)
        case _ => (null, false)
      }
      case _ => (null, false)
    }
  }
}