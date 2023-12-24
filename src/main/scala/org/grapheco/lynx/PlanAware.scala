package org.grapheco.lynx

import org.grapheco.lynx.logical.plans.LogicalPlan
import org.grapheco.lynx.physical.plans.PhysicalPlan
import org.opencypher.v9_0.ast.Statement

trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LogicalPlan

  def getPhysicalPlan(): PhysicalPlan

  def getOptimizerPlan(): PhysicalPlan
}
