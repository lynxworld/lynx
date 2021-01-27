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
  private def safeBinaryOp(lhs: Expression, rhs: Expression, op: (LynxValue, LynxValue) => LynxValue)(implicit ec: ExpressionContext): LynxValue = {
    eval(lhs) match {
      case LynxNull => LynxNull
      case lvalue =>
        (lvalue, eval(rhs)) match {
          case (_, LynxNull) => LynxNull
          case (lvalue, rvalue) => op(lvalue, rvalue)
        }
    }
  }

  override def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expr match {
      case Add(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a + b
            case (a: LynxString, b: LynxString) => LynxString(a.v + b.v)
          })

      case Subtract(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a - b
          })

      case Ors(exprs) =>
        LynxBoolean(exprs.exists(eval(_) == LynxBoolean(true)))

      case Ands(exprs) =>
        LynxBoolean(exprs.forall(eval(_) == LynxBoolean(true)))

      case Or(lhs, rhs) =>
        LynxBoolean(Seq(lhs, rhs).exists(eval(_) == LynxBoolean(true)))

      case And(lhs, rhs) =>
        LynxBoolean(Seq(lhs, rhs).forall(eval(_) == LynxBoolean(true)))

      case NotEquals(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          LynxValue(lvalue != rvalue))

      case Equals(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          LynxValue(lvalue == rvalue))

      case GreaterThan(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
          }
        })

      case GreaterThanOrEqual(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() >= b.number.doubleValue())
          }
        })

      case LessThan(lhs, rhs) =>
        eval(GreaterThan(rhs, lhs)(expr.position))

      case LessThanOrEqual(lhs, rhs) =>
        eval(GreaterThanOrEqual(rhs, lhs)(expr.position))

      case Not(in) =>
        eval(in) match {
          case LynxNull => LynxNull
          case LynxBoolean(b) => LynxBoolean(!b)
        }

      case v: Literal =>
        LynxValue(v.value)

      case Variable(name) =>
        ec.vars(name)

      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case LynxNull => LynxNull
          case cn: LynxNode => cn.property(name).get
          case cr: LynxRelationship => cr.property(name).get
        }

      case Parameter(name, parameterType) =>
        LynxValue(ec.param(name))
      case CaseExpression(expression, alternatives, default) => {
        val expr = alternatives.find(alt=>eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse(default.get)
        eval(expr)
      }
      case MapExpression(items) => LynxMap(items.map(it => it._1.name->eval(it._2)).toMap)
    }
}