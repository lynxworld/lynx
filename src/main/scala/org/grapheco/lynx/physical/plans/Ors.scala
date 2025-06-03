package org.grapheco.lynx.physical.plans
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.{LynxType, LynxValue}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/5/13 16:09
 * @Modified By:
 */
case class Ors()(physicalPlans: Seq[PhysicalPlan], val plannerContext: PhysicalPlannerContext) extends PhysicalPlan {

  override var left: Option[PhysicalPlan] = physicalPlans.headOption
  override var right: Option[PhysicalPlan] = physicalPlans.lastOption

  override def children: Seq[PhysicalPlan] = physicalPlans

  override def schema: Seq[(String, LynxType)] = physicalPlans.headOption.get.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val dataFrames = physicalPlans.map(_.execute(ctx))
    DataFrame(dataFrames.head.schema, () => dataFrames.map(_.records).reduceOption(_ ++ _).getOrElse(Iterator.empty))
  }
}
