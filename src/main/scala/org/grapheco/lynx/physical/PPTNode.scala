package org.grapheco.lynx.physical

import org.grapheco.lynx.context.ExecutionContext
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.{LynxType, TreeNode}

trait PPTNode extends TreeNode {
  override type SerialType = PPTNode
  override val children: Seq[PPTNode] = Seq.empty
  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PPTNode]): PPTNode
}
