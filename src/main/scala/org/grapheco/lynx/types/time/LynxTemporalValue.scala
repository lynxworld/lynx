package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger

import java.util.Date

trait LynxTemporalValue extends LynxValue {
  def timestamp: LynxInteger = LynxInteger(new Date().getTime())
}
