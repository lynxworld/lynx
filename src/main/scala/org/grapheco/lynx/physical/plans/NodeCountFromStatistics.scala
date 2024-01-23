package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxNodeLabel

case class NodeCountFromStatistics(label: Option[LynxNodeLabel], variableName: String)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {
  override val schema: Seq[(String, LynxType)] = Seq((variableName, LynxInteger(0).lynxType))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val stat = plannerContext.runnerContext.graphModel.statistics
    val res = label.map(label => stat.numNodeByLabel(label)).getOrElse(stat.numNode)
    DataFrame(schema, () => Iterator(Seq(LynxInteger(res))))
  }
}
