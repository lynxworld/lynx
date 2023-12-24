package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.ReturnItems

case class PPTWith(ri: ReturnItems)(implicit in: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(in)

  override def withChildren(children0: Seq[PhysicalPlan]): PPTWith = PPTWith(ri: ReturnItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = in.execute(ctx)
}
