package org.grapheco.lynx

import org.grapheco.lynx.logical.plans.LogicalPlan
import org.grapheco.lynx.physical.PPTNode
import org.opencypher.v9_0.ast.Statement

trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LogicalPlan

  def getPhysicalPlan(): PPTNode

  def getOptimizerPlan(): PPTNode
}
