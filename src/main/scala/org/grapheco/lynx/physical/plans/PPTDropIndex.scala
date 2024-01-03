package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.util.symbols.CTAny

case class PPTDropIndex(labelName: String, properties: List[String])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.dropIndex(labelName, properties.toSet)
    DataFrame.empty
  }

  override val schema: Seq[(String, LynxType)] = {
    Seq("DropIndex" -> CTAny)
  }
}
