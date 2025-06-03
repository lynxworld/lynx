package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.{PhysicalPlannerContext, plans}
import org.grapheco.lynx.physical.plans.{PhysicalPlan, RelationshipScan, Reverse}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/5/8 14:38
 * @Modified By:
 */
object RelationshipScanRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, {
    case rel: RelationshipScan => if(rel.leftNode.properties.isEmpty && rel.rightNode.properties.nonEmpty)
      Reverse()(RelationshipScan(rel.rel, rel.rightNode, rel.leftNode, rel.optional)(rel.plannerContext),rel.plannerContext)
    else rel
  })
}
