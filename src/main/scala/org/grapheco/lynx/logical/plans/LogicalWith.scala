package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItems

case class LogicalWith(ri: ReturnItems)(val in: LogicalPlan) extends SingleLogicalPlan(Some(in))
