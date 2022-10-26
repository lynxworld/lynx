package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.time.LynxComponentDate.getYearMonthDay
import org.grapheco.lynx.types.time.LynxComponentTime.{getHourMinuteSecond, getNanosecond}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.getZone
import org.grapheco.lynx.util.{LynxTemporalParseException, LynxTemporalParser}
import org.opencypher.v9_0.util.symbols.CTLocalDateTime

import java.time.LocalDateTime
import java.util.{Calendar, GregorianCalendar}

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

  override def sameTypeCompareTo(o: LynxValue): Int = ???


  //LynxComponentDate
  val calendar = new GregorianCalendar()
  calendar.set(Calendar.YEAR, localDateTime.getYear)
  calendar.set(Calendar.DAY_OF_YEAR, localDateTime.getDayOfYear)
  var year: Int = localDateTime.getYear
  var month: Int = localDateTime.getMonthValue
  var quarter: Int = month - month % 3
  var week: Int = calendar.getWeeksInWeekYear
  var weekYear: Int = calendar.getWeekYear
  var ordinalDay: Int = localDateTime.getDayOfYear
  val calendar_quarterBegin = new GregorianCalendar()
  calendar_quarterBegin.set(Calendar.YEAR, localDateTime.getYear)
  calendar_quarterBegin.set(Calendar.MONTH, quarter)
  calendar_quarterBegin.set(Calendar.DAY_OF_MONTH, 1)
  var dayOfQuarter: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var quarterDay: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var day: Int = localDateTime.getDayOfMonth
  var dayOfWeek: Int = calendar.get(Calendar.DAY_OF_WEEK)
  var weekDay: Int = calendar.get(Calendar.DAY_OF_WEEK)

  //LynxComponentTime
  var hour: Int = localDateTime.getHour
  var minute: Int = localDateTime.getMinute
  var second: Int = localDateTime.getSecond
  var microsecond: Int = localDateTime.getNano * Math.pow(0.1, 6).toInt
  var millisecond: Int = (localDateTime.getNano * Math.pow(0.1, 3) % Math.pow(10, 3)).toInt
  var nanosecond: Int = localDateTime.getNano % Math.pow(10, 3).toInt
  var fraction: Int = localDateTime.getNano
}

object LynxLocalDateTime extends LynxTemporalParser {
  def now(): LynxLocalDateTime = LynxLocalDateTime(LocalDateTime.now())

  def of(localDateTime: LocalDateTime): LynxLocalDateTime = LynxLocalDateTime(localDateTime)

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nanosecond: Int): LynxLocalDateTime =
    LynxLocalDateTime(LocalDateTime.of(year, month, day, hour, minute, second, nanosecond))


  def parse(localDateTimeStr: String): LynxLocalDateTime =
    LynxLocalDateTime(LocalDateTime.parse(localDateTimeStr))
  def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalDateTime = null
    if (map.contains("timezone")) {
      if (map.size == 1) {
       of(LocalDateTime.now(getZone(map("timezone").asInstanceOf[String])))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("year")) {
      val (year, month, day) = getYearMonthDay(map)
      val (hour, minute, second) = getHourMinuteSecond(map, requiredHasDay = true)
      val nanoOfSecond = getNanosecond(map,requiredHasSecond = true)
      of(year, month, day, hour, minute, second, nanoOfSecond)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (year, month, day) ")
  }
}
