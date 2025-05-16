package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.dataframe.{InnerJoin, JoinType}


case class LogicalAndThen(joinType: JoinType = InnerJoin)(val first: LogicalPlan, val _then: LogicalPlan) extends LogicalPlan(Some(first), Some(_then))
