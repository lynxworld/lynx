package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.property.LynxInteger

trait LynxId {
  val value: Any

  def toLynxInteger: LynxInteger
}
