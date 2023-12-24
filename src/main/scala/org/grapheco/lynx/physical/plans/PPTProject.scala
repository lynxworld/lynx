package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.{ReturnItem, ReturnItems, ReturnItemsDef}

case class PPTProject(ri: ReturnItemsDef)(implicit val in: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(in)

  override def withChildren(children0: Seq[PhysicalPlan]): PPTProject = PPTProject(ri)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = ri.items.map(x => x.name -> x.expression).map { col =>
    col._1 -> typeOf(col._2, in.schema.toMap)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.project(ri.items.map(x => x.name -> x.expression))(ctx.expressionContext)
  }

  def withReturnItems(items: Seq[ReturnItem]) = PPTProject(ReturnItems(ri.includeExisting, items)(ri.position))(in, plannerContext)
}
