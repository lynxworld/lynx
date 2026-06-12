package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalProcedureCall}
import org.opencypher.v9_0.ast.{ProcedureResult, ProcedureResultItem, UnresolvedCall, Where}
import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName, Variable}

/////////////////ProcedureCall/////////////
case class CallTranslator(c: UnresolvedCall) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val UnresolvedCall(ns@Namespace(parts: List[String]), pn@ProcedureName(name: String), declaredArguments: Option[Seq[Expression]], declaredResult: Option[ProcedureResult]) = c
    val call = LogicalProcedureCall(ns, pn, declaredArguments)

    declaredResult match {
      case Some(ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where])) =>
        PipedTranslators(Seq(LPTSelectTranslator(items.map(
          item => {
            val ProcedureResultItem(output, Variable(varname)) = item
            varname -> output.map(_.name)
          }
        )), WhereTranslator(where))).translate(Some(call))

      case None => call
    }
  }
}
