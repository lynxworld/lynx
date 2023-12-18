package org.grapheco.lynx.logical.plans

case class LogicalUnion(distinct: Boolean)(val a: LogicalPlan, val b: LogicalPlan) extends LogicalPlan(Some(a), Some(b))
