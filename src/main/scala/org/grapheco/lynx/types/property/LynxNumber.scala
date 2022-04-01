package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue

trait LynxNumber extends LynxValue {
  def number: Number

  def +(that: LynxNumber): LynxNumber

  def -(that: LynxNumber): LynxNumber

}
