package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.SetClause

case class LogicalSetClause(d: SetClause)(val in: Option[LogicalPlan]) extends LogicalPlan(in, None)
