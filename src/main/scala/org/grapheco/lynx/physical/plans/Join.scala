package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, JoinType}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.opencypher.v9_0.expressions.Expression

/*
 @param joinType: InnerJoinPPTApply/FullJoin/LeftJoin/RightJoin
 */
case class Join(filterExpr: Option[Expression],
                isSingleMatch: Boolean,
                joinType: JoinType)
               (l: PhysicalPlan, r: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends DoublePhysicalPlan(l,r) {
  //  override val children: Seq[PhysicalPlan] = Seq(a, b)

  val a:PhysicalPlan = this.left.get
  val b:PhysicalPlan = this.right.get

  override def schema: Seq[(String, LynxType)] = a.schema.filterNot(col => b.schema.map(_._1).contains(col._1)) ++ b.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx.withArguments(DataFrame.cached(df1.schema, df1.records.toSeq)))
    df2
  }

}
