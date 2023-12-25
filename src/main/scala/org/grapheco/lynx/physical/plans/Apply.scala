package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.{ReturnItem, ReturnItems}

case class Apply(ri: Seq[ReturnItem])(from: PhysicalPlan, applyTo: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(from, applyTo)
  override val schema: Seq[(String, LynxType)] = applyTo.schema ++ from.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val _df1 = from.execute(ctx)
    val df1 = DataFrame.cached(_df1.schema, _df1.records.toArray.toSeq)
    val df2 = applyTo.execute(ctx.withArguments(df1))
    val j = df2.join(df1, isSingleMatch = true, InnerJoin)
    j
  }

  override def withChildren(children0: Seq[PhysicalPlan]): Apply = Apply(ri: Seq[ReturnItem])(children0.head, children0(1), plannerContext)
}
