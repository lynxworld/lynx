package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItem

case class LogicalAndThen(ri: Seq[ReturnItem])(val first: LogicalPlan, val _then: LogicalPlan) extends LogicalPlan(Some(first), Some(_then))
