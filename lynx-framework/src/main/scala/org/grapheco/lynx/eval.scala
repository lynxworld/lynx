package org.grapheco.lynx

import org.opencypher.v9_0.expressions._

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue
}

case class ExpressionContext(params: Map[String, LynxValue], vars: Map[String, LynxValue] = Map.empty) {
  def param(name: String): LynxValue = params(name)

  def var0(name: String): LynxValue = vars(name)

  def withVars(vars0: Map[String, LynxValue]): ExpressionContext = ExpressionContext(params, vars0)
}

class ExpressionEvaluatorImpl extends ExpressionEvaluator {
  override def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expr match {
      case Equals(lhs, rhs) =>
        LynxValue(eval(lhs) == eval(rhs))

      case GreaterThan(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (LynxNull, _) => LynxNull
          case (_, LynxNull) => LynxNull
          case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
        }

      case GreaterThanOrEqual(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (LynxNull, _) => LynxNull
          case (_, LynxNull) => LynxNull
          case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() >= b.number.doubleValue())
        }

      case LessThan(lhs, rhs) =>
        eval(GreaterThan(rhs, lhs)(expr.position))

      case LessThanOrEqual(lhs, rhs) =>
        eval(GreaterThanOrEqual(rhs, lhs)(expr.position))

      case v: Literal =>
        LynxValue(v.value)

      case Variable(name) =>
        ec.vars(name)

      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case cn: LynxNode => cn.property(name).get
          case cr: LynxRelationship => cr.property(name).get
        }

      case Parameter(name, parameterType) =>
        LynxValue(ec.param(name))
    }
}