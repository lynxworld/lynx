package org.grapheco.lynx.logical.plans

case class LogicalDistinct()(val in: LogicalPlan) extends SingleLogicalPlan(Some(in))
