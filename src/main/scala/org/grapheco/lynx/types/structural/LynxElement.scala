package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.traits.HasProperty

trait LynxElement extends LynxValue with HasProperty {
  val id: LynxId
}
