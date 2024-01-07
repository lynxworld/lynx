package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.opencypher.v9_0.expressions.Expression

case class PPTFilter(expr: Expression)(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan(l) {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val ec = ctx.expressionContext
    df.filter {
      (record: Seq[LynxValue]) =>
        eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
          case LynxBoolean(b) => b
          case LynxList(l) => l.nonEmpty
          case LynxNull => false //todo check logic
        }
    }(ec)
  }

}
