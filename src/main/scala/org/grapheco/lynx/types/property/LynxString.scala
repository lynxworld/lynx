package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{LTString, LynxValue, StringType, TypeMismatchException}

/**
 * @ClassName LynxString
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxString(v: String) extends LynxValue {
  def value: String = v

  def lynxType: StringType = LTString

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case s: LynxString => value.compareTo(s.value)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }

  override def toString: String = v
}
