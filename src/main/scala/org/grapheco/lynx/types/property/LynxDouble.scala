package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTFloat, FloatType}

/**
 * @ClassName LynxDouble
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDouble(v: Double) extends LynxNumber {
  def value: Double = v

  def number: Number = v

  def cypherType: FloatType = CTFloat

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxDouble(v + v2)
      case LynxDouble(v2) => LynxDouble(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxDouble(v - v2)
      case LynxDouble(v2) => LynxDouble(v - v2)
    }
  }

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxDouble].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxDouble].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxDouble].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxDouble].value
}
