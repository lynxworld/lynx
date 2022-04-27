package org.grapheco.lynx.physical

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.context.{ExecutionContext, ExpressionContext, PhysicalPlannerContext}
import org.grapheco.lynx.dataframe.{DataFrame, DataFrameOps}
import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.Expression

trait AbstractPPTNode extends PPTNode {

  val plannerContext: PhysicalPlannerContext

  implicit def ops(ds: DataFrame): DataFrameOps = DataFrameOps(ds)(plannerContext.runnerContext.dataFrameOperator)

  val typeSystem = plannerContext.runnerContext.typeSystem
  val graphModel = plannerContext.runnerContext.graphModel
  val expressionEvaluator = plannerContext.runnerContext.expressionEvaluator
  val procedureRegistry = plannerContext.runnerContext.procedureRegistry

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType = plannerContext.runnerContext.expressionEvaluator.typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(expressionEvaluator, ctx.expressionContext)
  }
}
