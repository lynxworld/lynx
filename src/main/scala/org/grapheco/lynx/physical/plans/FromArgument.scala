package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LynxType}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class FromArgument(arguments: Seq[(String, LynxType)])(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = arguments

  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    ctx.arguments.select(arguments.map{case (name, _) => (name, None)})
  }

  override def toString: String = s"FromArgument(${arguments.map(_._1).mkString(",")})"
}
