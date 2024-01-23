package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class Select(columns: Seq[(String, Option[String])])(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {

  override def schema: Seq[(String, LynxType)] = columns.map(x => x._2.getOrElse(x._1)).map(x => x -> in.schema.find(_._1 == x).get._2) //fixme

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.select(columns)
  }
}
