package org.grapheco.lynx

import org.opencypher.v9_0.expressions.{Expression, Parameter}

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): CypherValue
}

trait ExpressionContext {
  def get(name: String): Any
}

case class ExpressionContextImpl(params: Map[String, Any]) extends ExpressionContext {
  def get(name: String): Any = params(name)
}

class ExpressionEvaluatorImpl extends ExpressionEvaluator {
  override def eval(expr: Expression)(implicit ec: ExpressionContext): CypherValue =
    expr match {
      case Parameter(name, parameterType) =>
        CypherValue(ec.get(name))
    }
}