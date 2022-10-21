package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.time.LynxComponentDate.{getYearMonthDay, transformDate, transformYearOrdinalDay, transformYearQuarterDay, transformYearWeekDay}
import org.grapheco.lynx.types.time.LynxComponentTime.{getHourMinuteSecond, getNanosecond}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.{getOffset, getZone}
import org.grapheco.lynx.util.{LynxTemporalParseException, LynxTemporalParser}
import org.grapheco.lynx.util.LynxTemporalParser.splitDateTime
import org.opencypher.v9_0.util.symbols.{CTDateTime, DateTimeType}

import java.sql.Timestamp
import java.time.{LocalDateTime, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.{Calendar, Date, GregorianCalendar}

/**
 * @ClassName LynxDateTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDateTime(zonedDateTime: ZonedDateTime) extends LynxTemporalValue with LynxComponentDate with LynxComponentTime with LynxComponentTimeZone {
  def value: ZonedDateTime = zonedDateTime

  def lynxType: DateTimeType = CTDateTime

  override def sameTypeCompareTo(o: LynxValue): Int = ???


  //LynxComponentDate
  val calendar = new GregorianCalendar()
  calendar.set(Calendar.YEAR, zonedDateTime.getYear)
  calendar.set(Calendar.DAY_OF_YEAR, zonedDateTime.getDayOfYear)
  var year: Int = zonedDateTime.getYear
  var month: Int = zonedDateTime.getMonthValue
  var quarter: Int = month - month % 3
  var week: Int = calendar.getWeeksInWeekYear
  var weekYear: Int = calendar.getWeekYear
  var ordinalDay: Int = zonedDateTime.getDayOfYear
  val calendar_quarterBegin = new GregorianCalendar()
  calendar_quarterBegin.set(Calendar.YEAR, zonedDateTime.getYear)
  calendar_quarterBegin.set(Calendar.MONTH, quarter)
  calendar_quarterBegin.set(Calendar.DAY_OF_MONTH, 1)
  var dayOfQuarter: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var quarterDay: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var day: Int = zonedDateTime.getDayOfMonth
  var dayOfWeek: Int = calendar.get(Calendar.DAY_OF_WEEK)
  var weekDay: Int = calendar.get(Calendar.DAY_OF_WEEK)

  //LynxComponentTime
  var hour: Int = zonedDateTime.getHour
  var minute: Int = zonedDateTime.getMinute
  var second: Int = zonedDateTime.getSecond
  var millisecond: Int = (zonedDateTime.getNano * Math.pow(0.1, 6)).toInt
  var microsecond: Int = (zonedDateTime.getNano * Math.pow(0.1, 3) - millisecond * Math.pow(10, 3)).toInt
  var nanosecond: Int = zonedDateTime.getNano % Math.pow(10, 3).toInt
  var fraction: Int = zonedDateTime.getNano

  //LynxComponentZone
  var timeZone: String = zonedDateTime.getOffset.getId
  var offset: String = zonedDateTime.getOffset.toString
  var offsetMinutes: Int = zonedDateTime.getOffset.getTotalSeconds / 60
  var offsetSeconds: Int = zonedDateTime.getOffset.getTotalSeconds


  var epochMillis: Long = zonedDateTime.toInstant.toEpochMilli
  var epochSeconds: Long = zonedDateTime.toInstant.toEpochMilli * Math.pow(0.1, 3).toInt


}


object LynxDateTime extends LynxTemporalParser {

  def now(): LynxDateTime = LynxDateTime(ZonedDateTime.now())

  def now(zoneId: ZoneId): LynxDateTime = LynxDateTime(ZonedDateTime.now(zoneId))

  def of(zonedDateTimeStr: String): LynxDateTime = {
    try {
      LynxDateTime.of(zonedDateTimeStr)
    } catch {
      case _ => throw new Exception("DateTimeOfException")
    }
  }

  def of(zonedDateTime: ZonedDateTime): LynxDateTime = LynxDateTime(zonedDateTime)

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nanosecond: Int, timezone: String): LynxDateTime =
    LynxDateTime(ZonedDateTime.of(year, month, day, hour, minute, second, nanosecond, LynxComponentTimeZone.getZone(timezone)))


  def parse(zonedDateTimeStr: String, zoneId: ZoneId): LynxDateTime = {
    //    val map = splitDateTime(zonedDateTimeStr)
    //    map.size match {
    //      case 3 => of(getYearMonthDay(map.get("dateStr").get), getHourMinuteSecond(map.get("timeStr").get), zoneId.getId)
    //      case 4 => of(getYearMonthDay(map.get("dateStr").get), getHourMinuteSecond(map.get("timeStr").get), zoneId.getId, map.get("utcStr").get)
    //    }
    try {
      val v = ZonedDateTime.parse(zonedDateTimeStr).toLocalDateTime.atZone(zoneId)
      LynxDateTime(v)
    } catch {
      case _ => throw new Exception("DateTimeParseException")
    }
  }

  def parse(zonedDateTimeStr: String): LynxDateTime = {
    val map = splitDateTime(zonedDateTimeStr)
    val dateTuple = getYearMonthDay(map.get("dateStr").get)
    val timeTuple = getHourMinuteSecond(map.get("timeStr").get)
    val zoneStr = getZone(map.get("zoneStr").getOrElse(null))
    val offsetStr = getOffset(map.get("offsetStr").getOrElse(null))

    var dateStr = dateTuple._1.formatted("%04d") + "-" + dateTuple._2.formatted("%02d") + "-" + dateTuple._3.formatted("%02d")
    var timeStr = timeTuple._1.formatted("%02d") + ":" + timeTuple._2.formatted("%02d") + ":" + timeTuple._3.formatted("%02d") + (timeTuple._4 match {
      case 0 => ""
      case v: Int => "." + v.toString
    })
    var dateTime = LocalDateTime.parse(dateStr + "T" + timeStr)
    zoneStr match {
      case null => LynxDateTime(dateTime.atOffset(ZoneOffset.of(offsetStr)).toZonedDateTime)
      case v: ZoneId => LynxDateTime(dateTime.atZone(v))
    }
  }

  def parse(map: Map[String, Any]): LynxDateTime = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }

    val zoneId = getZone(map.getOrElse("timezone", "Z") match {
      case v: String => v.replace(" ", "_")
    })
    if (map.size == 1) {
      map match {
        case m if m.contains("timezone") => LynxDateTime.now(zoneId)
        case m if m.contains("datetime") => LynxDateTime.parse(m.get("datetime").get.toString)
        case m if m.contains("epochSeconds") => LynxDateTime(ZonedDateTime.ofInstant(new Timestamp(LynxDateTime.now().epochMillis).toInstant(), zoneId))
        case m if m.contains("epochMillis") => LynxDateTime(ZonedDateTime.ofInstant(new Timestamp(LynxDateTime.now().epochSeconds).toInstant(), zoneId))
      }
    } else {
      var v: ZonedDateTime = null
      val (year, month, day) = map match {
        case m if m.contains("dayOfWeek") => transformYearWeekDay(m)
        case m if m.contains("dayOfQuarter") => transformYearQuarterDay(m)
        case m if m.contains("ordinalDay") => transformYearOrdinalDay(m)
        case m if m.contains("date") => transformDate(m)
        case _ => getYearMonthDay(map)
      }

      val (hour, minute, second) = map match {
        case m if m.contains("time") =>
          val v = m("time").asInstanceOf[LocalTime]
          (v.getHour, v.getMinute,
            if (m.contains("second")) m("second") match {
              case v: Long => v.toInt
              case v: LynxInteger => v.value.toInt
            }
            else v.getSecond
          )
        case _ => getHourMinuteSecond(map, false)
      }
      val nanoOfSecond = map match {
        case m if m.contains("time") => m("time") match {
          case v: LocalTime => v.getNano
          case v: LynxInteger => v.value.toInt
        }
        case _ => getNanosecond(map, false)
      }
      of(year, month, day, hour, minute, second, nanoOfSecond, zoneId.getId)
    }
  }

}