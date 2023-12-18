package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.Pattern

case class LogicalCreate(c: Pattern)(val in: Option[LogicalPlan]) extends SingleLogicalPlan(in)
