package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.{LynxType, TreeNode}

//logical plan tree node (operator)
abstract class LogicalPlan(override var left: Option[LogicalPlan], override var right: Option[LogicalPlan]) extends TreeNode {

  override type SerialType = LogicalPlan

}

abstract class LeafLogicalPlan extends LogicalPlan(None, None)

abstract class SingleLogicalPlan(in: Option[LogicalPlan]) extends LogicalPlan(in, None)
