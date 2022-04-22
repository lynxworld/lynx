package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.opencypher.v9_0.util.symbols.CTLocalDateTime

import java.time.LocalDateTime

/**
 * @ClassName LynxLocalDateTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxLocalDateTime(localDateTime: LocalDateTime) extends LynxTemporalValue {
  def value: LocalDateTime = localDateTime

  def lynxType: LynxType = CTLocalDateTime
}
