package org.grapheco.lynx.physical

trait PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode
}
