package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{AnyType, LTAny, LynxValue, TypeMismatchException}

/**
 * @ClassName LynxNull
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
object LynxNull extends LynxValue {
  override def value: Any = null

  override def lynxType: AnyType = LTAny

  override def toString: String = "null"

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case LynxNull => 0
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
