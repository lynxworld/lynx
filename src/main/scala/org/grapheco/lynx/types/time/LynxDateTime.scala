package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTDateTime, DateTimeType}

import java.time.ZonedDateTime

/**
 * @ClassName LynxDateTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDateTime(zonedDateTime: ZonedDateTime) extends LynxTemporalValue {
  def value: ZonedDateTime = zonedDateTime

  def lynxType: DateTimeType = CTDateTime

  override def sameTypeCompareTo(o: LynxValue): Int = ???
}
