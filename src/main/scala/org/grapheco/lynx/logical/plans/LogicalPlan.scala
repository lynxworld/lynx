package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.TreeNode

//logical plan tree node (operator)
abstract class LogicalPlan(override var left: Option[LogicalPlan], override var right: Option[LogicalPlan]) extends TreeNode {

  override type SerialType = LogicalPlan

  def alone: LogicalPlan = {this.left=None;this.right=None;this}

}

abstract class LeafLogicalPlan extends LogicalPlan(None, None)

abstract class SingleLogicalPlan(in: Option[LogicalPlan]) extends LogicalPlan(in, None)
