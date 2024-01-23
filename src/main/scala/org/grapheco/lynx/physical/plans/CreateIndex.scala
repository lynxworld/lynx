package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.util.symbols.CTAny

case class CreateIndex(labelName: String, properties: List[String])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName, properties.toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> CTAny)
  }
}
