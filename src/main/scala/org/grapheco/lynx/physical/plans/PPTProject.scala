package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.{ReturnItem, ReturnItems, ReturnItemsDef}

case class PPTProject(ri: ReturnItemsDef)(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {

  override def schema: Seq[(String, LynxType)] = ri.items.map(x => x.name -> x.expression).map { col =>
    col._1 -> typeOf(col._2, in.schema.toMap)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.project(ri.items.map(x => x.name -> x.expression))(ctx.expressionContext)
  }

//  def withReturnItems(items: Seq[ReturnItem]) = PPTProject(ReturnItems(ri.includeExisting, items)(ri.position))(in, plannerContext)
}
