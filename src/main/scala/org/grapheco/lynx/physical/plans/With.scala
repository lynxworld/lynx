package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.ReturnItems

case class With(ri: ReturnItems)(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {
  override def schema: Seq[(String, LynxType)] = ri.items.map(x => x.name)
    .map(x => x -> in.schema.toMap.get(x).get)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    in.execute(ctx).select(ri.items.map(x => x.name -> None))
  }
}
