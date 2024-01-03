package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.opencypher.v9_0.expressions.Expression

case class PPTOrderBy(sortItem: Seq[SortItem])(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec: ExpressionContext = ctx.expressionContext
    /*    val sortItems:Seq[(String ,Boolean)] = sortItem.map {
          case AscSortItem(expression) => (expression.asInstanceOf[Variable].name, true)
          case DescSortItem(expression) => (expression.asInstanceOf[Variable].name, false)
        }*/
    val sortItems2: Seq[(Expression, Boolean)] = sortItem.map {
      case AscSortItem(expression) => (expression, true)
      case DescSortItem(expression) => (expression, false)
    }
    df.orderBy(sortItems2)(ec)
  }

}
