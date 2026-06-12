package org.grapheco.lynx.types.traits

import org.grapheco.lynx.types.LynxValue

trait LynxComputable {
  def add(another: LynxValue): LynxValue

  def subtract(another: LynxValue): LynxValue

  def multiply(another: LynxValue): LynxValue

  def divide(another: LynxValue): LynxValue
}
