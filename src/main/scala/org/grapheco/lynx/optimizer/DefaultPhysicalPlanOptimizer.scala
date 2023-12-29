package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan
import org.grapheco.lynx.runner.CypherRunnerContext

import scala.language.implicitConversions

/**
 * @ClassName DefaultPhysicalPlanOptimizer
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultPhysicalPlanOptimizer(runnerContext: CypherRunnerContext) extends PhysicalPlanOptimizer {
  val rules = Seq[PhysicalPlanOptimizerRule](
//    RemoveNullProject,
    PPTFilterPushDownRule,
//    JoinReferenceRule,
//    JoinTableSizeEstimateRule,
//    StatisticsRule
  )

  implicit def ops(p: PhysicalPlan): OperablePhysicalPlan = new OperablePhysicalPlan(p)

  def optimize(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    rules.foldLeft(plan)((optimized, rule) => rule.apply(optimized, ppc))
  }
}
