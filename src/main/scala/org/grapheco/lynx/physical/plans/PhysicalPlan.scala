package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.{LynxType, TreeNode}

trait PhysicalPlan extends TreeNode {

  override type SerialType = PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq.empty

  var left: Option[PhysicalPlan]

  var right: Option[PhysicalPlan]

  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = withChildren(children0.headOption, children0.lastOption)

  def withChildren(left: Option[PhysicalPlan], right: Option[PhysicalPlan] = None): PhysicalPlan = {
    this.left = left
    this.right = right
    this
  }
}

