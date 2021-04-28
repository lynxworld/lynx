package org.grapheco.lynx

import org.grapheco.lynx.func.LynxProcedure
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation, FunctionName, LogicalVariable, Namespace}
import org.opencypher.v9_0.util.InputPosition

class DefaultFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }
  @LynxProcedure(name = "sum")
  def sum(x: LynxInteger): LynxInteger = {
    x
  }
  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): LynxInteger = {
    LynxInteger(math.pow(x.value, n.value).toInt)
  }
}

case class FunctionExpression(val funcInov: FunctionInvocation)(implicit plannerContext: LogicalPlannerContext) extends Expression {
  val FunctionInvocation(namespace: Namespace, functionName: FunctionName, distinct: Boolean, args: IndexedSeq[Expression]) = funcInov
  val procedure: CallableProcedure = plannerContext.runnerContext.procedureRegistry.getProcedure(namespace.parts, functionName.name).get

  override def position: InputPosition = funcInov.position

  override def productElement(n: Int): Any = funcInov.productElement(n)

  override def productArity: Int = funcInov.productArity

  override def canEqual(that: Any): Boolean = funcInov.canEqual(that)
}