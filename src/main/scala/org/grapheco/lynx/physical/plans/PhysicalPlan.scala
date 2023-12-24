package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.{LynxType, TreeNode}

trait PhysicalPlan extends TreeNode {

  override type SerialType = PhysicalPlan

  override val children: Seq[PhysicalPlan] = Seq.empty

  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan
}

