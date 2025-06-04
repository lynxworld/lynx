package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTAny, LynxType}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext

case class CreateIndex(labelName: String, properties: List[String])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName, properties.toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> LTAny)
  }
}

case class DropIndex(labelName: String, properties: List[String])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.dropIndex(labelName, properties.toSet)
    DataFrame.empty
  }

  override val schema: Seq[(String, LynxType)] = {
    Seq("DropIndex" -> LTAny)
  }
}
