package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.ReturnItem

case class CreateUnit(items: Seq[ReturnItem])(val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override def withChildren(children0: Seq[PhysicalPlan]): CreateUnit = CreateUnit(items)(plannerContext)

  override val schema: Seq[(String, LynxType)] =
    items.map(item => item.name -> typeOf(item.expression))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    createUnitDataFrame(items)
  }

}
