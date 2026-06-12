package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.Expression

case class LogicalLimit(expression: Expression)(val in: LogicalPlan) extends SingleLogicalPlan(Some(in))
