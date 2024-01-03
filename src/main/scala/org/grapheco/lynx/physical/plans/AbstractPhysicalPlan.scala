package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, DataFrameOps}
import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.procedure.ProcedureRegistry
import org.grapheco.lynx.runner.{ExecutionContext, GraphModel}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.Expression

import scala.language.implicitConversions

abstract class AbstractPhysicalPlan(override var left: Option[PhysicalPlan] = None,
                                    override var right: Option[PhysicalPlan] = None) extends PhysicalPlan {

  val plannerContext: PhysicalPlannerContext

  implicit def ops(ds: DataFrame): DataFrameOps = DataFrameOps(ds)(plannerContext.runnerContext.dataFrameOperator)

  val typeSystem: TypeSystem = plannerContext.runnerContext.typeSystem
  val graphModel: GraphModel = plannerContext.runnerContext.graphModel
  val expressionEvaluator: ExpressionEvaluator = plannerContext.runnerContext.expressionEvaluator
  val procedureRegistry: ProcedureRegistry = plannerContext.runnerContext.procedureRegistry

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType = expressionEvaluator.typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(expressionEvaluator, ctx.expressionContext)
  }
}

abstract class DoublePhysicalPlan(l: PhysicalPlan, r: PhysicalPlan) extends AbstractPhysicalPlan(Some(l), Some(r))

abstract class SinglePhysicalPlan(l: PhysicalPlan) extends AbstractPhysicalPlan(Some(l), None) {
  def in: PhysicalPlan = this.left.getOrElse(throw ExecuteException(s"Physical Plan ${this.getClass.getSimpleName} need child!"))

  override def schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = in.execute(ctx)

  override def withChildren(left: Option[PhysicalPlan], right: Option[PhysicalPlan]): PhysicalPlan = {
    if (left.isEmpty) throw ExecuteException(s"Physical Plan ${this.getClass.getSimpleName} need child!")
    else super.withChildren(left,right)
  }
}

abstract class LeafPhysicalPlan extends AbstractPhysicalPlan(None, None) {
  override def withChildren(left: Option[PhysicalPlan], right: Option[PhysicalPlan]): PhysicalPlan = this
}
