package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{HasProperty, LynxPropertyKey}

import java.util.Date

trait LynxTemporalValue extends LynxValue with HasProperty{
  def timestamp: LynxInteger = LynxInteger(new Date().getTime)

  override def keys: Seq[LynxPropertyKey] = ???

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = ???
}
