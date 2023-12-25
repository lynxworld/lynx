package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.{LynxType, TreeNode}

//logical plan tree node (operator)
abstract class LogicalPlan(left: Option[LogicalPlan], right: Option[LogicalPlan]) extends TreeNode {

  override type SerialType = LogicalPlan
  override val children: Seq[LogicalPlan] = Seq(left, right).flatten
}

abstract class LeafLogicalPlan extends LogicalPlan(None, None)

abstract class SingleLogicalPlan(in: Option[LogicalPlan]) extends LogicalPlan(in, None)
