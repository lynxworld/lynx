package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxRelationshipType

/////////STATISTICS//////////////
case class PPTRelationshipCountFromStatistics(ttype: Option[LynxRelationshipType], variableName: String)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val schema: Seq[(String, LynxType)] = Seq((variableName, LynxInteger(0).lynxType))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val stat = plannerContext.runnerContext.graphModel.statistics
    val res = ttype.map(ttype => stat.numRelationshipByType(ttype)).getOrElse(stat.numRelationship)
    DataFrame(schema, () => Iterator(Seq(LynxInteger(res))))
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = ???
}
