package org.opencypher.lynx

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._

object Eval {
  def eval(expr: Expr, record: CypherMap, properties: CypherMap): Any = {
    expr match {
      case _: Var | _: Param | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
        properties(expr.withoutType)

      case ElementProperty(NodeVar(name: String), key: PropertyKey) =>
        record(name).asInstanceOf[LynxNode].properties(key.name)

      case TrueLit =>
        true

      case FalseLit =>
        false
    }
  }
}
