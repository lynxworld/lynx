package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItem

case class LogicalApply(ri: Seq[ReturnItem])(val left: LogicalPlan, val right: LogicalPlan) extends LogicalPlan(Some(left), Some(right))
