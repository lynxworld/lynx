package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class Cross()(l: PhysicalPlan, r: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends
  DoublePhysicalPlan(l, r){

  override val schema: Seq[(String, LynxType)] = l.schema ++ r.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val lhs = this.left.get
    val rhs = this.right.get

    val df1 = lhs.execute(ctx)
    val df2 = rhs.execute(ctx)
    df1.cross(df2)
  }

}
