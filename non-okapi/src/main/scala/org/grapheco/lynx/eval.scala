package org.grapheco.lynx

import org.opencypher.v9_0.expressions.Expression

trait ExpressionEvaluator {
  def eval(expr: Expression): CypherValue
}

class ExpressionEvaluatorImpl extends ExpressionEvaluator {
  override def eval(expr: Expression): CypherValue = CypherInteger(1)
}