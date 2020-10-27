package org.opencypher.lynx.util

import org.opencypher.lynx.{LynxSession, RecordHeader}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBigDecimal, CypherBoolean, CypherFloat, CypherInteger, CypherMap, CypherNull, CypherValue, Element, Node}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions.ASTBlobLiteral

case class EvalContext(header: RecordHeader, valueOfColumn: (String) => CypherValue, parameters: CypherMap) {

}

trait ExpressionEvaluator {
  def aggregate(expr: Expr, values: Seq[CypherValue]): CypherValue

  def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue
}

class SimpleExpressionEvaluator(session: LynxSession) extends ExpressionEvaluator {
  def aggregate(expr: Expr, values: Seq[CypherValue]): CypherValue = {
    expr match {
      case CountStar => values.size
    }
  }

  def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue = {
    val EvalContext(header, valueOfColumn, parameters) = ctx

    expr match {
      case IsNull(expr) =>
        eval(expr) match {
          case CypherNull => true
          case _ => false
        }

      case IsNotNull(expr) =>
        eval(expr) match {
          case CypherNull => false
          case _ => true
        }

      case Not(expr) =>
        eval(expr) match {
          case CypherNull => CypherNull
          case CypherBoolean(x) => CypherBoolean(!x)
        }

      case Equals(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (CypherNull, _) => CypherNull
          case (_, CypherNull) => CypherNull
          case (x, y) => x.equals(y)
        }

      case GreaterThan(lhs: Expr, rhs: Expr) =>
        (eval(lhs), eval(rhs)) match {
          case (CypherBigDecimal(v1), CypherBigDecimal(v2)) => v1.compareTo(v2) > 0
          case (CypherInteger(v1), CypherInteger(v2)) => v1.compareTo(v2) > 0
          case (CypherFloat(v1), CypherFloat(v2)) => v1.compareTo(v2) > 0
          case (CypherNull, _) => CypherNull
        }

      case param: Param =>
        parameters(param.name)

      case _: Var | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
        //record(expr.withoutType)
        valueOfColumn(header.column(expr))

      case ep: ElementProperty =>
        (ep.key.name -> eval(ep.propertyOwner)) match {
          //TODO: lazy load of Element.property()
          case (name, e: Element[_]) => e.properties(name)
        }

      case AliasExpr(expr0: Expr, alias: Var) =>
        eval(expr0)

      case TrueLit =>
        true

      case FalseLit =>
        false

      case NullLit =>
        CypherNull

      case lit: ASTBlobLiteral =>
        CypherValue(lit.value)
    }
  }
}
