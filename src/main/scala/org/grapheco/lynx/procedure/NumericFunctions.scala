package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNumber}

/**
 * @ClassName NumericFunctions
 * @Description These functions all operate on numerical expressions only,
 * and will return an error if used on any other values.
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class NumericFunctions {
  @LynxProcedure(name = "abs")
  def abs(x: LynxNumber): LynxNumber = {
    x match {
      case i: LynxInteger => LynxInteger(math.abs(i.value))
      case d: LynxFloat => LynxFloat(math.abs(d.value))
    }
  }

  @LynxProcedure(name = "ceil")
  def ceil(x: LynxNumber): Double = {
    math.ceil(x.number.doubleValue())
  }

  @LynxProcedure(name = "floor")
  def floor(x: LynxNumber): Double = {
    math.floor(x.number.doubleValue())
  }

  @LynxProcedure(name = "rand")
  def rand(): Double = {
    math.random()
  }

  @LynxProcedure(name = "round")
  def round(x: LynxNumber): LynxFloat = {
    LynxFloat(math.round(x.number.doubleValue()))
  }

  @LynxProcedure(name = "round")
  def round(x: LynxNumber, precision: LynxInteger): Double = {
    val base = math.pow(10, precision.value)
    math.round(base * x.number.doubleValue()).toDouble / base
  }

  @LynxProcedure(name = "sign")
  def sign(x: LynxNumber): Double = {
    math.signum(x.number.doubleValue())
  }

  @LynxProcedure(name = "trunc")
  def trunc(x: LynxNumber, precision: Option[LynxNumber] = None): Double = {
    precision match {
      case Some(p) =>
        val scale = math.pow(10, p.number.doubleValue()).toDouble
        math.floor(x.number.doubleValue() * scale) / scale
      case None =>
        if (x.number.doubleValue() >= 0) math.floor(x.number.doubleValue()) else math.ceil(x.number.doubleValue())
    }
  }
}
