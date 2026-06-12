package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{FloatType, LTFloat, LynxValue}

/**
 * @ClassName LynxDouble
 * @Description
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxFloat(v: Double) extends LynxNumber {
  def value: Double = v

  def number: Number = v

  def lynxType: FloatType = LTFloat

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v + v2)
      case LynxFloat(v2) => LynxFloat(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v - v2)
      case LynxFloat(v2) => LynxFloat(v - v2)
    }
  }

  // TODO the type of result
  override def *(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v*v2) // TODO: over
      case LynxFloat(v2) => LynxFloat(v*v2)
    }
  }

  override def /(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v/v2)
      case LynxFloat(v2) => LynxFloat(v/v2)
    }
  }

}
object LynxFloat{
  val ZERO: LynxFloat = LynxFloat(0)
}
