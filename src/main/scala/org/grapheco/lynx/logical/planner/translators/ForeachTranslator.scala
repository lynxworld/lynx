package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.LogicalPlan
import org.opencypher.v9_0.ast.{Foreach, SetClause}

///////////////Foreach////////////////
case class ForeachTranslator(f: Foreach) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val Foreach(variable, expression, updates) = f

    val translatedClauses = updates.map {
      case setClause: SetClause => {
        SetTranslator(setClause)
      }
      case _ => throw new Exception("clause in foreach not support:") //todo : support others clauses
    }
    PipedTranslators(translatedClauses).translate(in)
  }
}
