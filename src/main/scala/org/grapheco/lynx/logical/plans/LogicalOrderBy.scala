package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.SortItem

case class LogicalOrderBy(sortItem: Seq[SortItem])(val in: LogicalPlan) extends LogicalPlan(Some(in), None)
