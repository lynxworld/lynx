package org.grapheco.lynx.types.traits

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.LynxPropertyKey

trait HasProperty {
  def keys: Seq[LynxPropertyKey]

  def property(propertyKey: LynxPropertyKey): Option[LynxValue]
}
