package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.Expression

case class LogicalDelete(expressions: Seq[Expression], forced: Boolean)(val in: LogicalPlan) extends LogicalPlan(Some(in), None)
