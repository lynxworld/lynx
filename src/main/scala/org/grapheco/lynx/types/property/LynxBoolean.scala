package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
import org.opencypher.v9_0.util.symbols.{BooleanType, CTBoolean}

/**
 * @ClassName LynxBoolean
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxBoolean(v: Boolean) extends LynxValue {
  def value: Boolean = v

  def lynxType: BooleanType = CTBoolean

  /*
  Booleans are compared such that false is less than true.
  Booleans are incomparable to any value that is not also a boolean.
   */
  override def compareTo(o: LynxValue): Int = o match {
    case boolean: LynxBoolean => v.compareTo(boolean.v)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
