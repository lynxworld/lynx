package org.grapheco.lynx.logical.plans


case class LogicalAndThen()(val first: LogicalPlan, val _then: LogicalPlan) extends LogicalPlan(Some(first), Some(_then))
