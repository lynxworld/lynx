package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.property.{LynxBoolean, LynxString}
import org.grapheco.lynx.types.time.LynxComponentDate.{getYearMonthDay, transformDate, transformYearOrdinalDay, transformYearQuarterDay, transformYearWeekDay, truncateDate}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.getZone
import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
import org.grapheco.lynx.util.LynxTemporalParseException
import org.opencypher.v9_0.util.symbols.{CTDate, DateType}
import org.scalacheck.Gen
import org.scalacheck.Gen.calendar

import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, GregorianCalendar}

/**
 * @ClassName LynxDate
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDate(localDate: LocalDate) extends LynxTemporalValue with LynxComponentDate {
  def value: LocalDate = localDate

  def lynxType: DateType = CTDate

  val calendar = new GregorianCalendar()
  calendar.set(Calendar.YEAR, localDate.getYear)
  calendar.set(Calendar.DAY_OF_YEAR, localDate.getDayOfYear)

  //Integer at least 4 digits
  var year: Int = localDate.getYear

  //Integer 1 to 12
  var month: Int = localDate.getMonthValue

  //Integer 1 to 4
  var quarter: Int = month - month % 3

  //Integer 1 to 53
  var week: Int = calendar.getWeeksInWeekYear

  //Integer at least 4 digits
  var weekYear: Int = calendar.getWeekYear

  //Integer 1 to 366
  var ordinalDay: Int = localDate.getDayOfYear

  //Integer 1 to 92
  val calendar_quarterBegin = new GregorianCalendar()
  calendar_quarterBegin.set(Calendar.YEAR, localDate.getYear)
  calendar_quarterBegin.set(Calendar.MONTH, quarter)
  calendar_quarterBegin.set(Calendar.DAY_OF_MONTH, 1)

  var dayOfQuarter: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var quarterDay: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1

  //Integer 1 to 31
  var day: Int = localDate.getDayOfMonth

  //Integer 1 to 7
  var dayOfWeek: Int = calendar.get(Calendar.DAY_OF_WEEK)
  var weekDay: Int = calendar.get(Calendar.DAY_OF_WEEK)

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case date: LynxDate => localDate.compareTo(date.localDate)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }

}

object LynxDate {
  def today: LynxDate = LynxDate(LocalDate.now())

  def now(): LynxDate = LynxDate(LocalDate.now())

  def now(zoneId: ZoneId): LynxDate = LynxDate(LocalDate.now(zoneId))

  def of(localDate: LocalDate): LynxDate = LynxDate(localDate)

  def of(year: Int, month: Int, day: Int): LynxDate = LynxDate(LocalDate.of(year, month, day))

  def of(tuple: Tuple3[Int, Int, Int]): LynxDate = LynxDate(LocalDate.of(tuple._1, tuple._2, tuple._3))

  def parse(dateStr: String): LynxDate = of(LynxComponentDate.getYearMonthDay(dateStr))
  //    LynxComponentDate.getYearMonthDay(dateStr) match {
  //      case v: (Int, Int, Int) => of(v._1, v._2, v._3)
  //    }

  def parse(map: Map[String, Any]): LynxDate = {
    if (map.contains("unitStr")) return of(truncateDate(map))
    if (map.contains("date")) return of(transformDate(map))
    if (map.contains("ordinalDay")) return of(transformYearOrdinalDay(map))
    if (map.contains("quarter")) return of(transformYearQuarterDay(map))
    if (map.contains("week")) return of(transformYearWeekDay(map))
    if (map.contains("year")) return of(getYearMonthDay(map))
    if (map.contains("timezone") || map.size == 1) {
      return LynxDate(LocalDate.now(ZoneId.of(map.get("timezone").get match {
        case v: LynxString => v.value.toString.replace(" ", "_")
      })))
    }
    //    now(ZoneId.of(map.get("timezone").toString.replace(" ", "_")))
    throw LynxTemporalParseException("can not parse date from map")
  }
}
