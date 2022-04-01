package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTString, StringType}

/**
 * @ClassName LynxString
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxString(v: String) extends LynxValue {
  def value: String = v

  def cypherType: StringType = CTString

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxString].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxString].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxString].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxString].value
}
