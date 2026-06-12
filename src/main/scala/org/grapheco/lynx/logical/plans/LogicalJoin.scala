package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.dataframe.JoinType

case class LogicalJoin(val isSingleMatch: Boolean, joinType: JoinType)(val a: LogicalPlan, val b: LogicalPlan)
  extends LogicalPlan(Some(a), Some(b))
