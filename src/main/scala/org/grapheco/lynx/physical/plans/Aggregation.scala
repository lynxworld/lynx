package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.ReturnItem

case class Aggregation(aggregations: Seq[ReturnItem], groupings: Seq[ReturnItem])(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan {

  override def schema: Seq[(String, LynxType)] =
    (groupings ++ aggregations).map(x => x.name -> typeOf(x.expression, in.schema.toMap))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.groupBy(groupings.map(x => x.name -> x.expression), aggregations.map(x => x.name -> x.expression))(ctx.expressionContext)
  }
}
