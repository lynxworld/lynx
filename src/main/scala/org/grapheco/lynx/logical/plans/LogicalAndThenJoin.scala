package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.dataframe.JoinType

case class LogicalAndThenJoin(val isSingleMatch: Boolean, joinType: JoinType)(val first: LogicalPlan, val _then: LogicalPlan) extends LogicalPlan(Some(first), Some(_then))
