package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LynxType}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class FromArgument(cols: Seq[String])(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  override val schema: Seq[(String, LynxType)] = cols.map((_, LTNode)) // Fixme: hard code

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    if(ctx.arguments.schema.isEmpty) ctx.arguments else
      ctx.arguments.select(cols.map((_, None)))
  }
}
