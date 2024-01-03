package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class PPTDistinct()(l: PhysicalPlan,implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan(l) {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override val schema: Seq[(String, LynxType)] = in.schema
}
