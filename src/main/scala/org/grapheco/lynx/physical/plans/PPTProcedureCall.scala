package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.procedure.{UnknownProcedureException, WrongArgumentException}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxNull
import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName}

case class PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  val Namespace(parts: List[String]) = procedureNamespace
  val ProcedureName(name: String) = procedureName
  val arguments = declaredArguments.getOrElse(Seq.empty)
  val procedure = procedureRegistry.getProcedure(parts, name, arguments.size)
    .getOrElse {
      throw UnknownProcedureException(parts, name)
    }

  override def schema: Seq[(String, LynxType)] = procedure.outputs

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val args = declaredArguments match {
      case Some(args) => args.map(eval(_)(ctx.expressionContext))
      case None => procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
    }
    val argsType = args.map(_.lynxType)
    if (procedure.checkArgumentsType(argsType)) {
      DataFrame(procedure.outputs, () => Iterator(Seq(procedure.execute(args))))
    } else {
      throw WrongArgumentException(name, procedure.inputs.map(_._2), argsType)
    }
  }
}
