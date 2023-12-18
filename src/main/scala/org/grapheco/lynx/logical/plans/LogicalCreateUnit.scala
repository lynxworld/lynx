package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.ast.ReturnItem

case class LogicalCreateUnit(items: Seq[ReturnItem]) extends LogicalPlan(None, None)
