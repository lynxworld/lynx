package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, DataFrameOps}
import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.procedure.{ProcedureExpression, ProcedureRegistry}
import org.grapheco.lynx.runner.filter.FilterExpr
import org.grapheco.lynx.runner.{ExecutionContext, GraphModel, filter}
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.{Equals, Expression, In, ListLiteral, Property, Variable}

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
  def getNodeFilerProperties(properties: Option[Expression], ec: ExpressionContext): Option[FilterExpr] = {
    properties match {
      case None => None
      case pn@Some(ListLiteral(list)) => Some(filter.Ands(list.map(toFilerExpr(_)(ec)).toSet))
    }
  }
  def toFilerExpr(expression: Expression)(implicit ec: ExpressionContext): FilterExpr = {
    expression match {
      case e@Equals(lhs, rhs) => lhs match {
        case Property(Variable(name), pkn) => filter.Equals(LynxPropertyKey(pkn.name), LynxValue(eval(rhs)))
        case Variable(name) => eval(rhs) match {
          case n: LynxNode => filter.Equals(LynxPropertyKey("_lynx_sys_id"), n.id.toLynxInteger)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${e}")
        }
        case p: ProcedureExpression => filter.Equals(LynxPropertyKey("_lynx_sys_id"), LynxValue(eval(rhs)))
      }
      case in@In(lhs, rhs) => lhs match {
        case Property(Variable(name), pkn) => eval(rhs) match {
          case l: LynxList => filter.In(LynxPropertyKey(pkn.name), l)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
        case Variable(name) => eval(rhs) match {
          case LynxList(l: List[LynxNode]) => filter.In(LynxPropertyKey("_lynx_sys_id"), LynxList(l.map(_.id.toLynxInteger)))
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
        case p: ProcedureExpression => eval(rhs) match {
          case l: LynxList => filter.In(LynxPropertyKey("_lynx_sys_id"), l)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
      }
      case _ => throw new Exception(s"transferNodePatternToFilter fail ${expression}")
    }
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
