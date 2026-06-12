package org.grapheco.lynx.logical.plans

case class LogicalCross()(val a: LogicalPlan, val b: LogicalPlan) extends LogicalPlan(Some(a), Some(b))

