package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalAndThen, LogicalPlan, LogicalUnwind, LogicalWith}
import org.opencypher.v9_0.ast.{AliasedReturnItem, Unwind}

//////////////UNWIND//////////////////
case class UnwindTranslator(u: Unwind) extends LogicalTranslator {
  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    in match {
      case Some(w:LogicalWith) => LogicalAndThen()(w, LogicalUnwind(u))
      case _ => LogicalUnwind(u)(in)
    }
  }
}
