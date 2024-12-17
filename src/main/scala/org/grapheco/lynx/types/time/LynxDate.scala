package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.time.LynxComponentDate._
import org.grapheco.lynx.types.{DateType, LTDate, LynxValue, TypeMismatchException}
import org.grapheco.lynx.util.LynxTemporalParseException

import java.time.temporal.ChronoUnit
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

  def lynxType: DateType = LTDate

  def plusDuration(that: LynxDuration): LynxDate = {
//    var aVal = localDate
//    that.map.foreach(f => {
//      /*LynxDate not support time calculation with granularity below day.*/
//      val timeUnit = f._1 match {
//        case "years" => ChronoUnit.YEARS
//        case "months" => ChronoUnit.MONTHS
//        case "days" => ChronoUnit.DAYS
//        case _ => null
//      }
//      if (timeUnit != null)
//        aVal = aVal.plus(f._2.toLong, timeUnit)
//    })
//    LynxDate(aVal)
    LynxDate(value.plusYears(that.years).plusMonths(that.monthsOfYear).plusDays(that.days))
  }


  def minusDuration(that: LynxDuration): LynxDate = {
//    var aVal = localDate
//    that.map.foreach(f => {
//      /*LynxDate not support time calculation with granularity below day.*/
//      val timeUnit = f._1 match {
//        case "years" => ChronoUnit.YEARS
//        case "months" => ChronoUnit.MONTHS
//        case "days" => ChronoUnit.DAYS
//        case _ => null
//      }
//      if (timeUnit != null)
//        aVal = aVal.minus(f._2.toLong, timeUnit)
//    })
//    LynxDate(aVal)
    LynxDate(value.minusYears(that.years).minusMonths(that.monthsOfYear).minusDays(that.days))
  }

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

  override def keys: Seq[LynxPropertyKey] = Seq("year", "quarter", "month", "week", "weekYear", "dayOfQuarter", "quarterDay", "day", "ordinalDay", "dayOfWeek", "weekDay").map(LynxPropertyKey)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = Some(propertyKey.value match {
    case "year" => LynxInteger(this.year)
    case "quarter" => LynxInteger(this.quarter)
    case "month" => LynxInteger(this.month)
    case "week" => LynxInteger(this.week)
    case "weekYear" => LynxInteger(this.weekYear)
    case "dayOfQuarter" => LynxInteger(this.dayOfQuarter)
    case "quarterDay" => LynxInteger(this.quarterDay)
    case "day" => LynxInteger(this.day)
    case "ordinalDay" => LynxInteger(this.ordinalDay)
    case "dayOfWeek" => LynxInteger(this.dayOfWeek)
    case "weekDay" => LynxInteger(this.weekDay)
    case _ => null
  })
}

object LynxDate {
  def today: LynxDate = LynxDate(LocalDate.now())

  def now(): LynxDate = LynxDate(LocalDate.now())

  def now(zoneId: ZoneId): LynxDate = LynxDate(LocalDate.now(zoneId))

  def of(localDate: LocalDate): LynxDate = LynxDate(localDate)

  def of(year: Int, month: Int, day: Int): LynxDate = LynxDate(LocalDate.of(year, month, day))

  def of(tuple: Tuple3[Int, Int, Int]): LynxDate = LynxDate(LocalDate.of(tuple._1, tuple._2, tuple._3))

  def parse(dateStr: String): LynxDate = of(LynxComponentDate.getYearMonthDay(dateStr))

  def parse(map: Map[String, Any]): LynxDate = {
    if (map.contains("unitStr")) return of(truncateDate(map))
    if (map.contains("date")) return of(transformDate(map))
    if (map.contains("ordinalDay")) return of(transformYearOrdinalDay(map))
    if (map.contains("quarter")) return of(transformYearQuarterDay(map))
    if (map.contains("week")) return of(transformYearWeekDay(map))
    if (map.contains("year")) return of(getYearMonthDay(map))
    if (map.contains("timezone") && map.size == 1) {
      return LynxDate(LocalDate.now(ZoneId.of(map("timezone") match {
        case v: LynxString => v.value.toString.replace(" ", "_")
      })))
    }
    throw LynxTemporalParseException(s"Missing required keys in the map to parse date. Map content: $map")//原来为throw LynxTemporalParseException("can not parse date from map") 更改后提供更详细的错误信息，指出是 Map 中缺少哪些关键信息导致无法解析
  }
}
