package org.grapheco.lynx.logical.plans

case class LogicalUnion(distinct: Boolean)(a: LogicalPlan, b: LogicalPlan) extends LogicalPlan(Some(a), Some(b))
