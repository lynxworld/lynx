package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, JoinType}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class Apply(joinType: JoinType = InnerJoin)(implicit val plannerContext: PhysicalPlannerContext) extends DoublePhysicalPlan{

  override def schema: Seq[(String, LynxType)] = this.left.get.schema ++ this.right.get.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val from = this.left.get
    val applyTo = this.right.get

    val _df1 = from.execute(ctx)
    val df1 = DataFrame.cached(_df1.schema, _df1.records.toArray.toSeq)
    val df2 = applyTo.execute(ctx.withArguments(df1))
//    val j = df2.join(df1, isSingleMatch = true, InnerJoin) //TODO inner?
//    val j = df1.cross(df2)
    val j = df1.join(df2, isSingleMatch = true, joinType)
    j
  }

}
