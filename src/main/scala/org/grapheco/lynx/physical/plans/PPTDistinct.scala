package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class PPTDistinct()(implicit in: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PPTDistinct = PPTDistinct()(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}
