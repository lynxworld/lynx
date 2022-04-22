package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.opencypher.v9_0.util.symbols.CTLocalTime

import java.time.LocalTime

/**
 * @ClassName LynxLocalTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxLocalTime(localTime: LocalTime) extends LynxTemporalValue {
  def value: LocalTime = localTime

  def lynxType: LynxType = CTLocalTime
}
