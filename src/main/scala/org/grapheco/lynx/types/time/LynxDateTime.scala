package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.time.LynxComponentDate._
import org.grapheco.lynx.types.time.LynxComponentTime.{getHourMinuteSecond, getNanosecond, truncateTime}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.{getOffset, getZone, truncateZone}
import org.grapheco.lynx.util.LynxTemporalParseException
import org.grapheco.lynx.util.LynxTemporalParser.splitDateTime
import org.opencypher.v9_0.util.symbols.{CTDateTime, DateTimeType}

import java.sql.Timestamp
import java.time._
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.{Calendar, GregorianCalendar}

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

  /*a mapping for time calculation */
  val timeUnit: Map[String, TemporalUnit] = Map(
    "years" -> ChronoUnit.YEARS, "months" -> ChronoUnit.MONTHS, "days" -> ChronoUnit.DAYS,
    "hours" -> ChronoUnit.HOURS, "minutes" -> ChronoUnit.MINUTES, "seconds" -> ChronoUnit.SECONDS,
    "milliseconds" -> ChronoUnit.MILLIS, "nanoseconds" -> ChronoUnit.NANOS
  )

  def plusDuration(that: LynxDuration): LynxDateTime = {
    var aVal = zonedDateTime
    that.map.foreach(f => aVal = aVal.plus(f._2.toLong, timeUnit.get(f._1).get))
    LynxDateTime(aVal)
  }

  def minusDuration(that: LynxDuration): LynxDateTime = {
    var aVal = zonedDateTime
    that.map.foreach(f => aVal = aVal.minus(f._2.toLong, timeUnit.get(f._1).get))
    LynxDateTime(aVal)
  }

  override def sameTypeCompareTo(o: LynxValue): Int = {
    o match {
      case x: LynxDateTime => zonedDateTime.compareTo(x.value)
      case _ => throw new Exception(s"expect type LynxDateTime,but find ${o.getClass.getTypeName}")
    }
  }


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

  var offset: String = zonedDateTime.getOffset.toString
  var timeZone: String = zonedDateTime.getZone.toString
  var offsetMinutes: Int = zonedDateTime.getOffset.getTotalSeconds / 60
  var offsetSeconds: Int = zonedDateTime.getOffset.getTotalSeconds


  var epochMillis: Long = zonedDateTime.toInstant.toEpochMilli
  var epochSeconds: Long = (zonedDateTime.toInstant.toEpochMilli * Math.pow(0.1, 3)).toInt

  override def keys: Seq[LynxPropertyKey] = super.keys ++ Seq("year", "quarter", "month", "week", "weekYear", "dayOfQuarter", "quarterDay", "day", "ordinalDay", "dayOfWeek", "weekDay", "timezone", "offset", "offsetMinutes", "offsetSeconds", "epochMillis", "epochSeconds", "hour", "minute", "second", "millisecond", "microsecond", "nanosecond").map(LynxPropertyKey)

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

    case "hour" => LynxInteger(this.hour)
    case "minute" => LynxInteger(this.minute)
    case "second" => LynxInteger(this.second)
    case "millisecond" => LynxInteger(this.millisecond)
    case "microsecond" => LynxInteger(this.microsecond + this.millisecond * Math.pow(10, 3).toLong)
    case "nanosecond" => LynxInteger(this.fraction)

    case "epochMillis" => LynxInteger(this.epochMillis)
    case "epochSeconds" => LynxInteger(this.epochSeconds)

    case "timezone" => LynxString(this.timeZone)
    case "offset" => LynxString(this.offset)
    case "offsetMinutes" => LynxInteger(this.offsetMinutes)
    case "offsetSeconds" => LynxInteger(this.offsetSeconds)
    case _ => null
  })
}


object LynxDateTime {

  def now(): LynxDateTime = LynxDateTime(ZonedDateTime.now())

  def now(zoneId: ZoneId): LynxDateTime = LynxDateTime(ZonedDateTime.now(zoneId))

  def of(zonedDateTimeStr: String): LynxDateTime = {
    try {
      LynxDateTime.of(zonedDateTimeStr)
    } catch {
      case _: Throwable => throw new Exception("DateTimeOfException")
    }
  }

  def of(zonedDateTime: ZonedDateTime): LynxDateTime = LynxDateTime(zonedDateTime)

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nanosecond: Int, timezone: String): LynxDateTime =
    LynxDateTime(ZonedDateTime.of(year, month, day, hour, minute, second, nanosecond, LynxComponentTimeZone.getZone(timezone)))


  def parse(zonedDateTimeStr: String, zoneId: ZoneId): LynxDateTime = {
    try {
      val v = ZonedDateTime.parse(zonedDateTimeStr).toLocalDateTime.atZone(zoneId)
      LynxDateTime(v)
    } catch {
      case _ => throw new Exception("DateTimeParseException")
    }
  }

  def parse(zonedDateTimeStr: String): LynxDateTime = {
    val map = splitDateTime(zonedDateTimeStr)
    val dateTuple = getYearMonthDay(map("dateStr"))
    val timeTuple = getHourMinuteSecond(map("timeStr"))
    val zoneStr = getZone(map.get("zoneStr").orNull)
    val offsetStr = getOffset(map.getOrElse("offsetStr", null))

    var dateStr = dateTuple._1.formatted("%04d") + "-" + dateTuple._2.formatted("%02d") + "-" + dateTuple._3.formatted("%02d")
    var timeStr = timeTuple._1.formatted("%02d") + ":" + timeTuple._2.formatted("%02d") + ":" + timeTuple._3.formatted("%02d") + (timeTuple._4 match {
      case 0 => ""
      case v: Int => "." + v.formatted("%09d")
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

    val zoneId = getZone(map.get("timezone").orNull match {
      case v: String => v.replace(" ", "_")
      case LynxString(v) => v.replace(" ", "_")
      case null => map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getZone.getId
        case null => "Z"
      }
    })

    if (map.contains("epochSeconds") || map.contains("epochMillis")) {
      val epochDateTime: LynxDateTime = map match {
        case m if m.contains("epochSeconds") =>
          LynxDateTime(ZonedDateTime.ofInstant(
            new Timestamp(map.getOrElse("epochSeconds", LynxDateTime.now().epochSeconds) match {
              case LynxInteger(v) => v
              case LynxFloat(v) => (v * Math.pow(10, 3)).toLong
              case v: Long => v
            }).toInstant, zoneId))
        case m if m.contains("epochMillis") =>
          LynxDateTime(ZonedDateTime.ofInstant(
            new Timestamp(map.getOrElse("epochMillis", LynxDateTime.now().epochMillis) match {
              case LynxInteger(v) => v
              case v: Long => v
            }).toInstant(), zoneId))
        case _ => null
      }
      if (map.contains("nanosecond")) {
        return of(epochDateTime.year, epochDateTime.month, epochDateTime.day, epochDateTime.hour, epochDateTime.minute, epochDateTime.second, map.getOrElse("nanosecond", null) match {
          case null => 0
          case v: Int => v
          case LynxInteger(v) => v.toInt
        }, zoneId.getId)
      } else return epochDateTime
    }
    if (map.size == 1) {
      map match {
        case m if m.contains("timezone") => LynxDateTime.now(zoneId)
        case m if m.contains("datetime") => LynxDateTime.parse(m("datetime") match {
          case LynxDateTime(v) => v.toString
        })
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

      val (hour: Int, minute: Int, second: Int) = map match {
        case m if m.contains("time") =>
          m("time") match {
            case v: LynxLocalTime => (v.hour, v.minute,
              if (m.contains("second")) m("second") match {
                case v: Long => v.toInt
                case v: LynxInteger => v.value.toInt
              }
              else v.second
            )
          }
        case _ => getHourMinuteSecond(map, requiredHasDay = false)
      }
      val nanoOfSecond: Int = map match {
        case m if m.contains("time") => (
          m.getOrElse("time", 0) match {
            case v: LocalTime => v.getNano
            case v: LynxInteger => v.value.toInt
            case LynxLocalTime(v) => v.getNano
          })
        case _ => getNanosecond(map, requiredHasSecond = false)
      }
      if (map.contains("timezone") && map.contains("datetime")) {
        val old_datetime = ZonedDateTime.of(year, month, day, hour, minute, second, nanoOfSecond, map("datetime") match {
          case LynxDateTime(v) => v.getZone
        })
        val new_datetime = old_datetime.withZoneSameInstant(zoneId)
        return LynxDateTime(new_datetime)
      }
      if (map.contains("unitStr")) {
        val (truncate_year, truncate_month, truncate_day) = truncateDate(map)
        val (truncate_hour, truncate_minute, truncate_second, truncate_nanoOfSecond) = truncateTime(map)
        return of(truncate_year, truncate_month, truncate_day, truncate_hour, truncate_minute, truncate_second, truncate_nanoOfSecond, truncateZone(map).getId)
      }
      of(year, month, day, hour, minute, second, nanoOfSecond, zoneId.getId)
    }
  }

}
