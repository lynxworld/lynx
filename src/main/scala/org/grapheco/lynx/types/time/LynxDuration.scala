package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.{DurationType, LTDuration, LynxValue}
import org.grapheco.lynx.types.property.{LynxInteger, LynxNull}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.time.LynxComponentDuration.{AVG_DAYS_OF_MONTH, SECOND_OF_DAY}
import org.grapheco.lynx.types.time.LynxDuration.{getDurationMap, toSecond}
import org.grapheco.lynx.types.traits.HasProperty

import java.math.BigDecimal
import java.time.temporal.{ChronoField, ChronoUnit, TemporalUnit}
import java.time.{Duration, LocalDate, LocalTime, ZoneOffset}
import java.util.regex.Pattern

/**
 * @ClassName LynxDuration
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDuration(duration: String, map: Map[String, Int] = Map("days" -> 0)) extends LynxTemporalValue with LynxComponentDuration with HasProperty{

  def nano: Long = getDurationMap(duration)._1 match {
    case "-" => (-1 * (toSecond(getDurationMap(getDurationMap(duration)._2)) * Math.pow(10, 9))).toLong
    case _ => (toSecond(getDurationMap(getDurationMap(duration)._2)) * Math.pow(10, 9)).toLong
  }

  def value: Duration = Duration.ofNanos(nano)

  override def toString: String = duration
  override def equals(obj: Any): Boolean = obj match {
    case that: LynxDuration =>
      val isEqual = this.duration == that.duration
      isEqual
    case _ => false
  }
  
  def +(that: LynxDuration): LynxDuration = LynxDuration.parse(value.plus(that.value).toString, true)

  def -(that: LynxDuration): LynxDuration = LynxDuration.parse(value.minus(that.value).toString, true)

  var duration_Map: Map[String, Int] = map
  /*a mapping for time calculation */
  val timeUnit: Map[String, TemporalUnit] = Map(
    "years" -> ChronoUnit.YEARS, "months" -> ChronoUnit.MONTHS, "days" -> ChronoUnit.DAYS,
    "hours" -> ChronoUnit.HOURS, "minutes" -> ChronoUnit.MINUTES, "seconds" -> ChronoUnit.SECONDS,
    "milliseconds" -> ChronoUnit.MILLIS, "nanoseconds" -> ChronoUnit.NANOS
  )

  def plusByMap(that: LynxDuration): LynxDuration = {
    var durationMap: Map[String, Double] = Map()
    timeUnit.foreach(f => {
      val tmp = map.getOrElse(f._1, 0) + that.map.getOrElse(f._1, 0)
      if (tmp != 0)
        durationMap += (f._1 -> tmp.toDouble)
    })
    LynxDuration.parse(durationMap)
  }

  def minusByMap(that: LynxDuration): LynxDuration = {
    var durationMap: Map[String, Double] = Map()
    timeUnit.foreach(f => {
      val tmp = map.getOrElse(f._1, 0) - that.map.getOrElse(f._1, 0)
      if (tmp != 0)
        durationMap += (f._1 -> tmp.toDouble)
    })
    LynxDuration.parse(durationMap)
  }

  def multiplyInt(that: LynxInteger): LynxDuration = {
    var durationMap: Map[String, Double] = Map();
    timeUnit.foreach(f => {
      val tmp = map.getOrElse(f._1, 0) * that.value
      if (tmp != 0)
        durationMap += (f._1 -> tmp.toDouble)
    })
    LynxDuration.parse(durationMap)
  }

  def divideInt(that: LynxInteger): LynxDuration = {
    val nanos = LynxDuration.toSecond(map) / that.value
    if (nanos - nanos.toLong == 0) {
      LynxDuration.parse(LynxDuration.standardType(nanos))
    } else {
      LynxDuration(LynxDuration.standardType(nanos - Math.pow(0.1, 9)))
    }
  }


  def lynxType: DurationType = LTDuration

  var months: Int = duration_Map.getOrElse("months", 0) + duration_Map.getOrElse("years", 0) * 12
  var years: Int = months / 12
  var monthsOfYear: Int = months % 12

  var quarters: Int = months / 3
  var quartersOfYear: Int = quarters / 4
  var monthsOfQuarter: Int = months - quarters * 3

  var days: Int = duration_Map.getOrElse("days", 0)
  var weeks: Int = days / 7
  var daysOfWeek: Int = days - weeks * 7

  var hours: Long = duration_Map.getOrElse("hours", 0).toLong
  var minutesOfHour: Int = duration_Map.getOrElse("minutes", 0)
  var secondsOfMinute: Int = duration_Map.getOrElse("seconds", 0)


  var minutes: Long = minutesOfHour + hours * 60
  var seconds: Long = secondsOfMinute + minutes * 60

  var nanoseconds: Long = duration_Map.getOrElse("nanoseconds", 0) + (duration_Map.getOrElse("microseconds", 0) * Math.pow(10, 3)).toLong +
    (map.getOrElse("milliseconds", 0) * Math.pow(10, 6)).toLong + (seconds * Math.pow(10, 9)).toLong
  var milliseconds: Long = (nanoseconds / Math.pow(10, 6)).toLong
  var microseconds: Long = (nanoseconds / Math.pow(10, 3)).toLong


  var millisecondsOfSecond: Long = (milliseconds % Math.pow(10, 3)).toLong
  var microsecondsOfSecond: Long = (microseconds % Math.pow(10, 6)).toLong
  var nanosecondsOfSecond: Long = (nanoseconds % Math.pow(10, 9)).toLong

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  override def keys: Seq[LynxPropertyKey] = Seq("years", "monthsOfYear", "quartersOfYear", "quarters", "months", "monthsOfQuarter",
    "days", "weeks", "daysOfWeek", "hours", "minutesOfHour", "secondsOfMinute", "minutes", "seconds", "nanoseconds", "milliseconds", "microseconds",
    "microsecondsOfSecond", "millisecondsOfSecond", "nanosecondsOfSecond").map(LynxPropertyKey)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = Some(propertyKey.value match {
    case "years" => LynxInteger(this.years)
    case "monthsOfYear" => LynxInteger(this.monthsOfYear)
    case "quartersOfYear" => LynxInteger(this.quartersOfYear)
    case "quarters" => LynxInteger(this.quarters)
    case "months" => LynxInteger(this.months)
    case "monthsOfQuarter" => LynxInteger(this.monthsOfQuarter)

    case "days" => LynxInteger(this.days)
    case "weeks" => LynxInteger(this.weeks)
    case "daysOfWeek" => LynxInteger(this.daysOfWeek)

    case "hours" => LynxInteger(this.hours)
    case "minutesOfHour" => LynxInteger(this.minutesOfHour)
    case "secondsOfMinute" => LynxInteger(this.secondsOfMinute)
    case "minutes" => LynxInteger(this.minutes)
    case "seconds" => LynxInteger(this.seconds)

    case "microseconds" => LynxInteger(this.microseconds)
    case "milliseconds" => LynxInteger(this.milliseconds)
    case "nanoseconds" => LynxInteger(this.nanoseconds)
    case "microsecondsOfSecond" => LynxInteger(this.microsecondsOfSecond)
    case "millisecondsOfSecond" => LynxInteger(this.millisecondsOfSecond)
    case "nanosecondsOfSecond" => LynxInteger(this.nanosecondsOfSecond)
    case _ => LynxNull
  })


}

object LynxDuration {

  def getRemainder(value: Double, base: String): Int = {
    base match {
      case "seconds" => (value % 60).toInt
      case "minutes" => ((value / 60) % 60).toInt
      case "hours" => ((value / Math.pow(60, 2)) % 24).toInt
      case "days" => (((value / (Math.pow(60, 2) * 24)) % 365) % 30).toInt
      case "months" => (((value / (Math.pow(60, 2) * 24)) % 365) / 30).toInt
      case "years" => (value / (Math.pow(60, 2) * 24 * 365)).toInt
    }
  }

  def toSecond(map: Map[String, Int]): Double = {
    val year_Second: Double = map.getOrElse("years", 0) match {
      case 0 => 0
      case v => v * 365 * SECOND_OF_DAY
    }
    val month_Second: Double = map.getOrElse("months", 0) match {
      case 0 => 0
      case v => v * 30 * SECOND_OF_DAY
    }
    val week_Days: Int = map.getOrElse("weeks", 0)

    val day_Second: Double = map.getOrElse("days", 0) match {
      case 0 if week_Days == 0 => 0
      case 0 => week_Days * 7 * SECOND_OF_DAY
      case v => (v + week_Days * 7) * SECOND_OF_DAY
    }
    val hour_Second: Double = map.getOrElse("hours", 0) match {
      case 0 => 0
      case v => v * Math.pow(60, 2)
    }
    val minute_Second: Double = map.getOrElse("minutes", 0) match {
      case 0 => 0
      case v => v * 60
    }
    val second_Second: Double = map.getOrElse("seconds", 0) match {
      case s => map.getOrElse("nanoseconds", 0) match {
        case 0 => s
        case n => s + (n / Math.pow(10, 9))
      }
      case 0 => 0
    }
    val s1 = year_Second + month_Second + day_Second + hour_Second + minute_Second + second_Second
    s1
  }

  def standardType(second: Double, scale: Int = 9, map: Map[String, Double] = null): String = {
    val decimal = new BigDecimal(second - second.toInt)
    val nanoSecond = decimal.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue

    var t_flag = ""
    var nano_Str = "S"
    nano_Str = nanoSecond match {
      case 0 => "S"
      case v => t_flag = "T"
        val tmp = (v + 1).toString.split("\\.")(1)
        if (tmp.length >= 9) {
          "." + tmp.substring(0, 9) + "S"
        } else {
          "." + tmp + "S"
        }
    }
    if (nano_Str == "S" && map != null && map.getOrElse("nanoseconds", 0) != 100) {
      val digit: Int = map.getOrElse("nanoseconds", 0).toString.split("\\.")(0).length
      val zeroDigit: String = "000000000" + map.getOrElse("nanoseconds", 0).toString.split("\\.")(0)
      nano_Str = map.getOrElse("nanoseconds", 0) match {
        case 0 => "S"
        case v: Double => t_flag = "T"
          "." + zeroDigit.substring(digit, digit + 9) + "S"
      }
    }
    val second_Str: String = getRemainder(second, "seconds") match {
      case 0 if t_flag == "T" => "0" + nano_Str
      case 0 => ""
      case v => t_flag = "T"
        v + nano_Str
    }
    val minute_Str: String = getRemainder(second, "minutes") match {
      case 0 => ""
      case v => t_flag = "T"
        v + "M"
    }
    val hour_Str: String = getRemainder(second, "hours") match {
      case 0 => ""
      case v => t_flag = "T"
        v + "H"
    }

    val day_Str: String = getRemainder(second, "days") match {
      case 0 => ""
      case v => v + "D"
    }

    val month_Str: String = getRemainder(second, "months") match {
      case 0 => ""
      case v => v + "M"
    }
    val year_Str: String = getRemainder(second, "years") match {
      case 0 => ""
      case v => v + "Y"
    }
    "P" + year_Str + month_Str + day_Str + t_flag + hour_Str + minute_Str + second_Str
  }

  def standardTypeDate(map: Map[String, Int]): String = {

    val year_Str: String = map.getOrElse("years", 0) match {
      case 0 => ""
      case v => v + "Y"
    }
    val month_Str: String = map.getOrElse("months", 0) match {
      case 0 => ""
      case v => v + "M"
    }
    val week_Str: Int = map.getOrElse("weeks", 0)

    val day_Str: String = map.getOrElse("days", 0) match {
      case 0 if week_Str == 0 => ""
      case 0 => (week_Str * 7) + "D"
      case v => (v + week_Str * 7) + "D"
    }
    //T
    var t_flag = ""
    val hour_Str: String = map.getOrElse("hours", 0) match {
      case 0 => ""
      case v => t_flag = "T"
        v + "H"
    }
    val minute_Str: String = map.getOrElse("minutes", 0) match {
      case 0 => ""
      case v => t_flag = "T"
        v + "M"
    }
    val second_Str: String = map.getOrElse("seconds", 0) match {
      case 0 => ""
      case s => map.getOrElse("nanoseconds", 0) match {
        case 0 => t_flag = "T"
          s + "S"
        case n => t_flag = "T"
          val zeroDigit: String = "000000000" + n
          val digit: Int = n.toString.length
          s + "." + zeroDigit.substring(digit, digit + 9).replaceAll("(0)+$", "") + "S"
      }

    }
    "P" + year_Str + month_Str + day_Str + t_flag + hour_Str + minute_Str + second_Str
  }

  def getDurationMap(map: Map[String, Double]): Map[String, Int] = {
    var year, month, week, day, hour, minute, second, millisecond, microsecond, nanosecond, nanoOfSecond: Double = 0.toDouble
    year = map.getOrElse("years", 0.toDouble)
    month = ((year - year.toInt) * 12) + map.getOrElse("months", 0.toDouble)
    week = map.getOrElse("weeks", 0.toDouble)
    day = (month - month.toInt) * AVG_DAYS_OF_MONTH + (week - week.toInt) * 7 + map.getOrElse("days", 0.toDouble)
    hour = (day - day.toInt) * 24 + map.getOrElse("hours", 0.toDouble)
    minute = (hour - hour.toInt) * 60 + map.getOrElse("minutes", 0.toDouble)
    second = (minute - minute.toInt) * 60 + map.getOrElse("seconds", 0.toDouble)
    millisecond = (second - second.toInt) * Math.pow(10, 3) + map.getOrElse("milliseconds", 0.toDouble)
    microsecond = (millisecond - millisecond.toInt) * Math.pow(10, 3) + map.getOrElse("microseconds", 0.toDouble)
    nanosecond = (microsecond - microsecond.toInt) * Math.pow(10, 3) + map.getOrElse("nanoseconds", 0.toDouble)
    nanoOfSecond = millisecond * Math.pow(0.1, 3) + microsecond * Math.pow(0.1, 6) + nanosecond * Math.pow(0.1, 9)

    Map("years" -> year.toInt, "months" -> month.toInt, "weeks" -> week.toInt, "days" -> day.toInt,
      //T
      "hours" -> hour.toInt, "minutes" -> minute.toInt, "seconds" -> second.toInt, "nanoseconds" -> (nanoOfSecond * Math.pow(10, 9)).toInt)
  }

  def getDurationMap(lynxDuration_Str: String): (String, Map[String, Double]) = {
    val datePattern = "^([-+]?)P([-+]?[0-9]+\\.?[0-9]*Y)?([-+]?[0-9]+\\.?[0-9]*M)?([-+]?[0-9]+\\.?[0-9]*W)?([-+]?[0-9]+\\.?[0-9]*D)?.*$".r
    val timePattern = "^.*T([-+]?[0-9]+\\.?[0-9]*H)?([-+]?[0-9]+\\.?[0-9]*M)?([-+]?[0-9]+\\.?[0-9]*S)?$".r
    val nullPattern = ".+(null)".r
    val valuePattern = "^([a-z]+)(.+)[A-Z]$".r
    val map = new scala.collection.mutable.HashMap[String, Double]
    val datePattern(sign, year, month, week, day) = lynxDuration_Str
    val date_arr = Array("years" + year, "months" + month, "weeks" + week, "days" + day)
    if (Pattern.compile(".+T.+").matcher(lynxDuration_Str).matches()) {
      val timePattern(hour, minute, second) = lynxDuration_Str
      val time_arr = Array("hours" + hour, "minutes" + minute, "seconds" + second)
      time_arr.foreach {
        case nullPattern() =>
        case valuePattern(key, value) => map += (key -> value.toDouble)
        case _ =>
      }
    }
    date_arr.foreach {
      case nullPattern() =>
      case valuePattern(key, value) => map += (key -> value.toDouble)
      case _ =>
    }
    sign match {
      case "-" => ("-", map.toMap)
      case _ => ("", map.toMap)
    }
  }

  def valid(lynxDuration_str: String): Boolean = {
    val lynxDuration_format = Pattern.compile(
      "([-+]?)P(?:(?:([-+]?[0-9]+)Y)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)W)?(?:([-+]?[0-9]+)D)?)" +
        "(T(?:([-+]?[0-9]+)H)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?")
    lynxDuration_format.matcher(lynxDuration_str).matches()
  }

  def parse(map: Map[String, Double]): LynxDuration = {
    val duration_Str = standardType(toSecond(getDurationMap(map)), 9, map)
    if (valid(duration_Str)) {
      var flag = false
      map.values.foreach(f => if (f < 0) {
        flag = true
      })
      if (flag) {
        var tmp = LynxDuration.standardTypeDate(map.mapValues(_.toInt))
        if (tmp.contains("T") && duration_Str.contains("T")) {
          tmp = tmp.split("T")(0) + "T" + duration_Str.split("T")(1)
          LynxDuration(tmp, map.mapValues(_.toInt))
        } else {
          LynxDuration(tmp, map.mapValues(_.toInt))
        }
      } else if (map.mapValues(_.toInt.toDouble).equals(map)) {
        LynxDuration(duration_Str, map.mapValues(_.toInt))
      } else {
        LynxDuration(duration_Str, getDurationMap(map))
      }
    } else {
      throw new Exception("lynxDuration_Str is not valid")
    }
  }

  def parse(lynxDuration_Str: String, standard: Boolean = true): LynxDuration = {
    if (valid(lynxDuration_Str) && standard) {
      LynxDuration(lynxDuration_Str, getDurationMap(lynxDuration_Str)._2.mapValues(getDurationMap(lynxDuration_Str)._1 match {
        case "-" => _.toInt * -1
        case "" => _.toInt * 1
      }))
    } else if (Pattern.compile(".+[A-Z]$").matcher(lynxDuration_Str).matches()) {
      getDurationMap(lynxDuration_Str)
      val ld = parse(getDurationMap(lynxDuration_Str)._2)
      LynxDuration(getDurationMap(lynxDuration_Str)._1 + ld.toString, ld.map.mapValues(getDurationMap(lynxDuration_Str)._1 match {
        case "-" => _ * -1
        case "" => _ * 1
      }))
    }
    else {
      val lynxLocalDateTime = LynxLocalDateTime.parse(lynxDuration_Str.replace("P", ""))
      val lynxDuration_map =
        Map("years" -> lynxLocalDateTime.year, "months" -> lynxLocalDateTime.month, "days" -> lynxLocalDateTime.day,
          "hours" -> lynxLocalDateTime.hour, "minutes" -> lynxLocalDateTime.minute, "seconds" -> lynxLocalDateTime.second, "nanoseconds" -> lynxLocalDateTime.fraction)
      if (valid(standardTypeDate(lynxDuration_map))) {
        LynxDuration(standardTypeDate(lynxDuration_map), lynxDuration_map)
      } else {
        throw new Exception("lynxDuration_Str is not valid")
      }
    }
  }

  def dateBetween(begin: LocalDate, end: LocalDate): Double = {
    if (begin == null || end == null) return 0.toDouble
    if ((end.getYear - begin.getYear) > 0 && (((end.getMonthValue - begin.getMonthValue) < 0) || (((end.getDayOfMonth - begin.getDayOfMonth) < 0) && ((end.getMonthValue - begin.getMonthValue - 1) < 0)))) {
      ((end.getYear - begin.getYear) * 365 + (end.getMonthValue - begin.getMonthValue) * 30 + (end.getDayOfMonth - begin.getDayOfMonth - 5)) * SECOND_OF_DAY
    } else {
      ((end.getYear - begin.getYear) * 365 + (end.getMonthValue - begin.getMonthValue) * 30 + (end.getDayOfMonth - begin.getDayOfMonth)) * SECOND_OF_DAY
    }
  }


  def timeBetween(begin: LocalTime, end: LocalTime): Double = {
    (end.getHour - begin.getHour) * 3600 + (end.getMinute - begin.getMinute) * 60 +
      (end.getSecond - begin.getSecond) + (end.getNano - begin.getNano) * Math.pow(0.1, 9)
  }

  def offsetBetween(begin: ZoneOffset, end: ZoneOffset): Double = {
    if (begin == null || end == null) return 0
    end.get(ChronoField.OFFSET_SECONDS) - begin.get(ChronoField.OFFSET_SECONDS)
  }

  def between(date_1: Any, date_2: Any): LynxDuration = {
    val scale: Int = 3
    val (begin_Date, begin_Time, begin_Zone, end_Date, end_Time, end_Zone) = LynxComponentDuration.between(date_1, date_2)
    val second_Between: Double = dateBetween(begin_Date, end_Date) + timeBetween(begin_Time, end_Time) - offsetBetween(begin_Zone, end_Zone)
    LynxDuration(standardType(second_Between, scale))
  }

  def betweenSeconds(date_1: Any, date_2: Any): LynxDuration = {
    LynxDuration(between(date_1, date_2).value.toString)
  }

  def betweenDays(date_1: Any, date_2: Any): LynxDuration = {
    val (begin_Date, begin_Time, _, end_Date, end_Time, _) = LynxComponentDuration.between(date_1, date_2)
    if (end_Date == null || begin_Date == null) return LynxDuration("PT0S")
    val days = end_Date.toEpochDay - begin_Date.toEpochDay
    if (days == 0) return LynxDuration("PT0S")
    if (timeBetween(begin_Time, end_Time) * days < 0) {
      if (days < 0) return LynxDuration("P" + (days + 1) + "D")
      if (days > 0) return LynxDuration("P" + (days - 1) + "D")
    }
    LynxDuration("P" + days + "D")
  }

  def betweenMonths(date_1: Any, date_2: Any): LynxDuration = {
    val (begin_Date, begin_Time, _, end_Date, end_Time, _) = LynxComponentDuration.between(date_1, date_2)
    if (end_Date == null || begin_Date == null) return LynxDuration("PT0S")
    var days = end_Date.toEpochDay - begin_Date.toEpochDay
    if (timeBetween(begin_Time, end_Time) * days < 0) {
      if (days < 0) days = days + 1
      else days = days - 1
    }
    val truncate_months = days - (days % 365) % 30
    if (truncate_months == 0) return LynxDuration("PT0S")
    LynxDuration(standardType(truncate_months * 24 * 3600))
  }
}
