package org.grapheco.lynx.logical

import org.grapheco.lynx.context.LogicalPlannerContext

//translates an ASTNode into a LPTNode, `in` as input operator
trait LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode
}
