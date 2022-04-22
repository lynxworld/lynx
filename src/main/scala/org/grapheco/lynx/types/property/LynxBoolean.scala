package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
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
}
