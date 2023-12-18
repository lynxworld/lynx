package org.grapheco.lynx.logical.planner

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.plans.LogicalPlan
import org.opencypher.v9_0.ast.Statement

trait LogicalPlanner {
  def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalPlan
}
