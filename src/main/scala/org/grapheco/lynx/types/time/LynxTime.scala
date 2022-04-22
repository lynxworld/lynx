package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.opencypher.v9_0.util.symbols.CTTime

import java.time.OffsetTime

/**
 * @ClassName LynxTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxTime(offsetTime: OffsetTime) extends LynxTemporalValue {
  def value: OffsetTime = offsetTime

  def lynxType: LynxType = CTTime
}
