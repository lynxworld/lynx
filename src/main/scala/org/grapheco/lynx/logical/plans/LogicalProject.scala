package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItemsDef

case class LogicalProject(ri: ReturnItemsDef)(val in: LogicalPlan) extends LogicalPlan(Some(in), None)
