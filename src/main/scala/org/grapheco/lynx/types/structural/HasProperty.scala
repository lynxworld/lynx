package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.LynxValue

trait HasProperty {
  def keys: Seq[LynxPropertyKey]

  def property(propertyKey: LynxPropertyKey): Option[LynxValue]
}
