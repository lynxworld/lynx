package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTAny, CypherType}

/**
 * @ClassName LynxNull
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
object LynxNull extends LynxValue {
  override def value: Any = null

  override def cypherType: CypherType = CTAny
}
