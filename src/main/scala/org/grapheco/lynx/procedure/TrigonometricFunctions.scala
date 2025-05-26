package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.property.LynxNumber

/**
 * @ClassName TrigonometricFunctions
 * @Description These functions all operate on numerical expressions only,
 *              and will return an error if used on any other values.
 *              All trigonometric functions operate on radians, unless otherwise specified.
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class TrigonometricFunctions {
  @LynxProcedure(name = "acos")
  def acos(x: LynxNumber): Double = {
    math.acos(x.number.doubleValue())
  }

  @LynxProcedure(name = "asin")
  def asin(x: LynxNumber): Double = {
    math.asin(x.number.doubleValue())
  }

  @LynxProcedure(name = "atan")
  def atan(x: LynxNumber): Double = {
    math.atan(x.number.doubleValue())
  }

  @LynxProcedure(name = "atan2")
  def atan2(x: LynxNumber, y: LynxNumber): Double = {
    math.atan2(x.number.doubleValue(), y.number.doubleValue())
  }

  @LynxProcedure(name = "cos")
  def cos(x: LynxNumber): Double = {
    math.cos(x.number.doubleValue())
  }

  @LynxProcedure(name = "cot")
  def cot(x: LynxNumber): Double = {
    1.0 / math.tan(x.number.doubleValue())
  }

  @LynxProcedure(name = "degrees")
  def degrees(x: LynxNumber): Double = {
    math.toDegrees(x.number.doubleValue())
  }

  @LynxProcedure(name = "haversin")
  def haversin(x: LynxNumber): Double = {
    1.0 / 2 * (1 - math.cos(x.number.doubleValue()))
  }

  @LynxProcedure(name = "pi")
  def pi(): Double = {
    Math.PI
  }

  @LynxProcedure(name = "radians")
  def radians(x: LynxNumber): Double = {
    math.toRadians(x.number.doubleValue())
  }

  @LynxProcedure(name = "sin")
  def sin(x: LynxNumber): Double = {
    math.sin(x.number.doubleValue())
  }

  @LynxProcedure(name = "tan")
  def tan(x: LynxNumber): Double = {
    math.tan(x.number.doubleValue())
  }

  @LynxProcedure(name = "sinh")
  def sinh(x: LynxNumber): Double = {
    math.sinh(x.number.doubleValue())
  }

  @LynxProcedure(name = "cosh")
  def cosh(x: LynxNumber): Double = {
    math.cosh(x.number.doubleValue())
  }

  @LynxProcedure(name = "tanh")
  def tanh(x: LynxNumber): Double = {
    math.tanh(x.number.doubleValue())
  }
}
