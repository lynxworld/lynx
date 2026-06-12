package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{BooleanType, LTBoolean, LynxValue, TypeMismatchException}

/**
 * @ClassName LynxBoolean
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxBoolean(v: Boolean) extends LynxValue {
  def value: Boolean = v

  def lynxType: BooleanType = LTBoolean

  /*
  Booleans are compared such that false is less than true.
  Booleans are incomparable to any value that is not also a boolean.
   */
  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case boolean: LynxBoolean => v.compareTo(boolean.v)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }

  override def toString: String = value.toString
}

object LynxBoolean {
  def TRUE: LynxBoolean = LynxBoolean(true)

  def FALSE: LynxBoolean = LynxBoolean(false)
}
