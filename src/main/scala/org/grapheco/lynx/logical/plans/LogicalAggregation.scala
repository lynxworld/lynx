package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItem

case class LogicalAggregation(aggregatings: Seq[ReturnItem],
                              groupings: Seq[ReturnItem])(val in: LogicalPlan) extends LogicalPlan(Some(in), None)
