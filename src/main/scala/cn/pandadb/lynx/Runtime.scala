package cn.pandadb.lynx

import org.opencypher.okapi.api.value.CypherValue.{CypherBigDecimal, CypherBoolean, CypherFloat, CypherInteger, CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._

case class EvalContext(record: CypherMap, properties: CypherMap) {

}

object Runtime {
  def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue = {
    val EvalContext(record, properties) = ctx

    expr match {
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
        properties(param.name)

      case _: Var | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
        record(expr.withoutType)

      case ElementProperty(NodeVar(name: String), key: PropertyKey) =>
        record(name) match {
          case node: LynxNode =>
            node.properties(key.name)
          case _ =>
            CypherNull
        }

      case AliasExpr(expr0: Expr, alias: Var) =>
        eval(expr0)

      case TrueLit =>
        CypherBoolean(true)

      case FalseLit =>
        CypherBoolean(false)
    }
  }
}
