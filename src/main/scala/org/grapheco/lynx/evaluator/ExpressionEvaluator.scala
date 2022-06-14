package org.grapheco.lynx.evaluator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.LynxType
import org.opencypher.v9_0.expressions.Expression

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue

  def aggregateEval(expr: Expression)(ecs: Seq[ExpressionContext]): LynxValue

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType
}
