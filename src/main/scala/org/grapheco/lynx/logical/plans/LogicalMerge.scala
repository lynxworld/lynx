package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.MergeAction
import org.opencypher.v9_0.expressions.Pattern

case class LogicalMerge(m: Pattern, a: Seq[MergeAction])(val in: Option[LogicalPlan]) extends SingleLogicalPlan(in)
