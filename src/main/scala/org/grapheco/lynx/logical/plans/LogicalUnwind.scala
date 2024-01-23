package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.Unwind

case class LogicalUnwind(u: Unwind)(implicit val in: Option[LogicalPlan] = None) extends SingleLogicalPlan(in)
