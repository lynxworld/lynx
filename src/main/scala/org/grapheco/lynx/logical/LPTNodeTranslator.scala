package org.grapheco.lynx.logical

//translates an ASTNode into a LPTNode, `in` as input operator
trait LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode
}
