package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class Apply()(l: PhysicalPlan, r: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends
  DoublePhysicalPlan(l, r){

  override def schema: Seq[(String, LynxType)] = this.left.get.schema.filterNot(col => this.right.get.schema.map(_._1).contains(col._1)) ++ this.right.get.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val from = this.left.get
    val applyTo = this.right.get

    val df1: DataFrame = from.execute(ctx)
    val df2 = applyTo.execute(ctx.withArguments(df1))
    DataFrame(schema, () => df2.records)
  }

}
