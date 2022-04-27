package org.grapheco.lynx.logical

import org.grapheco.lynx.TreeNode

//logical plan tree node (operator)
trait LPTNode extends TreeNode {
  override type SerialType = LPTNode
  override val children: Seq[LPTNode] = Seq.empty
}
