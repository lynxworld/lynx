package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.Remove

case class LogicalRemove(r: Remove)(val in: Option[LogicalPlan]) extends LogicalPlan(in, None)
