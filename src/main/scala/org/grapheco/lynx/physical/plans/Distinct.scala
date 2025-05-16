package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class Distinct()(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override val schema: Seq[(String, LynxType)] = in.schema
}
