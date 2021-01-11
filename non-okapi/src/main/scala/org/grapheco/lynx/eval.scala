package org.grapheco.lynx

import org.opencypher.v9_0.expressions.{Expression, Literal, Parameter, Property, PropertyKeyName, Variable}

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): CypherValue
}

case class ExpressionContext(params: Map[String, CypherValue], vars: Map[String, CypherValue] = Map.empty) {
  def param(name: String): CypherValue = params(name)

  def var0(name: String): CypherValue = vars(name)

  def withVars(vars0: Map[String, CypherValue]): ExpressionContext = ExpressionContext(params, vars0)
}

class ExpressionEvaluatorImpl extends ExpressionEvaluator {
  override def eval(expr: Expression)(implicit ec: ExpressionContext): CypherValue =
    expr match {
      case v: Literal => CypherValue(v.value)
      case Variable(name) => ec.vars(name)
      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case cn: CypherNode => cn.property(name).get
          case cr: CypherRelationship => cr.property(name).get
        }
      case Parameter(name, parameterType) =>
        CypherValue(ec.param(name))
    }
}