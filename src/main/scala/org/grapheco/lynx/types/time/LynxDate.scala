package org.grapheco.lynx.types.time

import org.opencypher.v9_0.util.symbols.{CTDate, DateType}

import java.time.LocalDate

/**
 * @ClassName LynxDate
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDate(localDate: LocalDate) extends LynxTemporalValue {
  def value: LocalDate = localDate

  def cypherType: DateType = CTDate
}
