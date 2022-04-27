package org.grapheco.lynx.physical

import org.grapheco.lynx.context.PhysicalPlannerContext

trait PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode
}
