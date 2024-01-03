package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxRelationshipType

case class PPTRelationshipCountFromStatistics(relType: Option[LynxRelationshipType], variableName: String)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {
  override val schema: Seq[(String, LynxType)] = Seq((variableName, LynxInteger(0).lynxType))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val stat = plannerContext.runnerContext.graphModel.statistics
    val res = relType.map(ttype => stat.numRelationshipByType(ttype)).getOrElse(stat.numRelationship)
    DataFrame(schema, () => Iterator(Seq(LynxInteger(res))))
  }

}
