package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

case class LogicalPatternMatch(optional: Boolean,
                               variableName: Option[String],
                               headNode: NodePattern,
                               chain: Seq[(RelationshipPattern, NodePattern)]) extends LeafLogicalPlan
//                              (in: Option[LogicalPlan]) extends SingleLogicalPlan(in)
