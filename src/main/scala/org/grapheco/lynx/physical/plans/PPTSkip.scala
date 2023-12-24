package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.expressions.Expression

case class PPTSkip(expr: Expression)(implicit in: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.skip(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PPTSkip = PPTSkip(expr)(children0.head, plannerContext)
}
