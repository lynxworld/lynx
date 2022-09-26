package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
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

  def lynxType: DateType = CTDate

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case date: LynxDate => localDate.compareTo(date.localDate)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}

object LynxDate {
  def today: LynxDate = LynxDate(LocalDate.now())


}
