package org.grapheco.lynx.procedure

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.runner.{CypherRunnerContext, ProcedureUnregisteredException}
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation}
import org.opencypher.v9_0.util.InputPosition

/**
 * @ClassName ProcedureExpression
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
case class ProcedureExpression(val funcInov: FunctionInvocation)(implicit runnerContext: CypherRunnerContext) extends Expression with LazyLogging {
  val procedure: CallableProcedure = runnerContext.procedureRegistry.getProcedure(funcInov.namespace.parts, funcInov.functionName.name, funcInov.args.size).getOrElse(throw ProcedureUnregisteredException(funcInov.name))
  val args: Seq[Expression] = funcInov.args
  val aggregating: Boolean = funcInov.containsAggregate
  val distinct: Boolean = funcInov.distinct

  logger.debug(s"binding FunctionInvocation ${funcInov.name} to procedure ${procedure}, containsAggregate: ${aggregating}")

  override def position: InputPosition = funcInov.position

  override def productElement(n: Int): Any = funcInov.productElement(n)

  override def productArity: Int = funcInov.productArity

  override def canEqual(that: Any): Boolean = funcInov.canEqual(that)

  override def containsAggregate: Boolean = funcInov.containsAggregate

  override def findAggregate: Option[Expression] = funcInov.findAggregate

}
