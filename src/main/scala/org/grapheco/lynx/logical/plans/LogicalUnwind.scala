package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.Unwind

case class LogicalUnwind(u: Unwind)(val in: Option[LogicalPlan]) extends LogicalPlan(in, None)
