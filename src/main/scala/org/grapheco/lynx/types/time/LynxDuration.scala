package org.grapheco.lynx.types.time

import com.sun.scenario.effect.Offset
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.time.LynxComponentDuration.{AVG_DAYS_OF_MONTH, AVG_DAYS_OF_YEAR, SECOND_OF_DAY, getDuration}
import org.joda.time.Period
import org.opencypher.v9_0.util.symbols.CTDuration

import java.math.BigDecimal
import java.time.{Duration, LocalDate, LocalTime, ZoneOffset}
import java.time.temporal.{ChronoField, TemporalField}
import java.util.Calendar
import java.util.regex.Pattern
import scala.collection.{breakOut, mutable}
import scala.util.control.Breaks.{break, breakable}

/**
 * @ClassName LynxDuration
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxDuration(duration: String) extends LynxTemporalValue with LynxComponentDuration {

  def value: Duration = getDuration(duration)

  override def toString: String = duration

  import java.util.regex.Pattern


  //  def durationValue: Duration = LynxComponentDuration.parse(duration)


  def +(that: LynxDuration): LynxDuration = LynxDuration(value.plus(that.value).toString)

  def -(that: LynxDuration): LynxDuration = LynxDuration(value.minus(that.value).toString)


  def lynxType: LynxType = CTDuration

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  //  override def toString: String = duration

  var year: Int = (value.toDays / AVG_DAYS_OF_YEAR).toInt
  var month: Int = ((value.toDays % AVG_DAYS_OF_YEAR) / AVG_DAYS_OF_MONTH).toInt
  var day: Int = (value.toDays % AVG_DAYS_OF_MONTH).toInt
  var hour: Int = (value.toHours % 24).toInt
  var minute: Int = (value.toMinutes % 60).toInt
  var second: Long = (value.toNanos % (6 * Math.pow(10, 4))).toLong


}

object LynxDuration {

  def getRemainder(value: Double, base: String): Int = {
    base match {
      case "second" => (value % 60).toInt
      case "minute" => ((value / 60) % 60).toInt
      case "hour" => ((value / Math.pow(60, 2)) % 24).toInt
      case "day" => ((value / (Math.pow(60, 2) * 24)) % 30).toInt
      case "month" => ((value / (Math.pow(60, 2) * 24 * 30)) % 12).toInt
      case "year" => (value / (Math.pow(60, 2) * 24 * 30 * 12)).toInt
    }
  }

  def toSecond(map: Map[String, Int]): Double = {
    val year_Second: Double = map.getOrElse("year", 0) match {
      case 0 => 0
      case v => v * 365 * SECOND_OF_DAY
    }
    val month_Second: Double = map.getOrElse("month", 0) match {
      case 0 => 0
      case v => v * 30 * SECOND_OF_DAY
    }
    val week_Days: Int = map.getOrElse("week", 0)

    val day_Second: Double = map.getOrElse("day", 0) match {
      case 0 if week_Days == 0 => 0
      case 0 => week_Days * 7 * SECOND_OF_DAY
      case v => (v + week_Days * 7) * SECOND_OF_DAY
    }
    val hour_Second: Double = map.getOrElse("hour", 0) match {
      case 0 => 0
      case v => v * Math.pow(60, 2)
    }
    val minute_Second: Double = map.getOrElse("minute", 0) match {
      case 0 => 0
      case v => v * 60
    }
    val second_Second: Double = map.getOrElse("second", 0) match {
      case 0 => 0
      case s => map.getOrElse("nanoOfSecond", 0) match {
        case 0 => s
        case n => s + (n / Math.pow(10, 9))
      }
    }
    year_Second + month_Second + day_Second + hour_Second + minute_Second + second_Second
  }

  def standardType(second: Double, scale: Int = 9): String = {
    val decimal = new BigDecimal(second - second.toInt)
    val nanoSecond = decimal.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue

    var t_flag = ""
    val nano_Str: String = nanoSecond match {
      case 0 => "S"
      case v => t_flag = "T"
        "." + v.toString.split("\\.")(1) + "S"
    }
    val second_Str: String = getRemainder(second, "second") match {
      case 0 if t_flag == "T" => "0" + nano_Str
      case 0 => ""
      case v => t_flag = "T"
        v + nano_Str
    }
    val minute_Str: String = getRemainder(second, "minute") match {
      case 0 => ""
      case v => t_flag = "T"
        v + "M"
    }
    val hour_Str: String = getRemainder(second, "hour") match {
      case 0 => ""
      case v => t_flag = "T"
        v + "H"
    }

    val day_Str: String = getRemainder(second, "day") match {
      case 0 => ""
      case v => v + "D"
    }

    val month_Str: String = getRemainder(second, "month") match {
      case 0 => ""
      case v => v + "M"
    }
    val year_Str: String = getRemainder(second, "year") match {
      case 0 => ""
      case v => v + "Y"
    }
    "P" + year_Str + month_Str + day_Str + t_flag + hour_Str + minute_Str + second_Str
  }

  def standardTypeDate(map: Map[String, Int]): String = {

    val year_Str: String = map.getOrElse("year", 0) match {
      case 0 => ""
      case v => v + "Y"
    }
    val month_Str: String = map.getOrElse("month", 0) match {
      case 0 => ""
      case v => v + "M"
    }
    val week_Str: Int = map.getOrElse("week", 0)

    val day_Str: String = map.getOrElse("day", 0) match {
      case 0 if week_Str == 0 => ""
      case 0 => (week_Str * 7) + "D"
      case v => (v + week_Str * 7) + "D"
    }
    //T
    var t_flag = ""
    val hour_Str: String = map.getOrElse("hour", 0) match {
      case 0 => ""
      case v => t_flag = "T"
        v + "H"
    }
    val minute_Str: String = map.getOrElse("minute", 0) match {
      case 0 => ""
      case v => t_flag = "T"
        v + "M"
    }
    val second_Str: String = map.getOrElse("second", 0) match {
      case 0 => ""
      case s => map.getOrElse("nanoOfSecond", 0) match {
        case 0 => t_flag = "T"
          s + "S"
        case n => t_flag = "T"
          s + "." + (n / Math.pow(10, 9)).toString.split("\\.")(1) + "S"
      }

    }
    "P" + year_Str + month_Str + day_Str + t_flag + hour_Str + minute_Str + second_Str
  }

  def getDurationString(lynxDuration: LynxDuration): String = {

    var year, month, day, hour, minute, second = ""
    if (lynxDuration.year != 0) {
      year = lynxDuration.year + "Y"
    }
    if (lynxDuration.month != 0) {
      month = lynxDuration.month + "M"
    }
    if (lynxDuration.day != 0) {
      day = lynxDuration.day + "D"
    }
    if (lynxDuration.hour != 0) {
      hour = lynxDuration.hour + "H"
    }
    if (lynxDuration.minute != 0) {
      minute = lynxDuration.minute + "M"
    }
    if (lynxDuration.second != 0) {
      second = lynxDuration.second + "S"
    }
    "P" + year + month + day + "T" + hour + minute + second
  }

  def valid(lynxDuration_str: String): Boolean = {
    val lynxDuration_format = Pattern.compile(
      "([-+]?)P(?:(?:([-+]?[0-9]+)Y)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)W)?(?:([-+]?[0-9]+)D)?)" +
        "(T(?:([-+]?[0-9]+)H)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?")
    lynxDuration_format.matcher(lynxDuration_str).matches()
  }

  def parse(map: Map[String, Double]): LynxDuration = {

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
    val lynxDuration_map =
      Map("year" -> year.toInt, "month" -> month.toInt, "week" -> week.toInt, "day" -> day.toInt,
        //T
        "hour" -> hour.toInt, "minute" -> minute.toInt, "second" -> second.toInt, "nanoOfSecond" -> (nanoOfSecond * Math.pow(10, 9)).toInt)
    val duration_Str = standardType(toSecond(lynxDuration_map))
    if (valid(duration_Str)) {
      LynxDuration(duration_Str)
    } else {
      throw new Exception("lynxDuration_Str is not valid")
    }
  }

  def parse(lynxDuration_Str: String): LynxDuration = {
    if (valid(lynxDuration_Str)) {
      LynxDuration(lynxDuration_Str)
    } else if (Pattern.compile(".+[A-Z]$").matcher(lynxDuration_Str).matches()) {

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
      parse(map.toMap).toString
      LynxDuration(sign + parse(map.toMap).toString)
    }
    else {
      val lynxLocalDateTime = LynxLocalDateTime.parse(lynxDuration_Str.replace("P", ""))
      val lynxDuration_map =
        Map("year" -> lynxLocalDateTime.year, "month" -> lynxLocalDateTime.month, "day" -> lynxLocalDateTime.day,
          "hour" -> lynxLocalDateTime.hour, "minute" -> lynxLocalDateTime.minute, "second" -> lynxLocalDateTime.second, "nanoOfSecond" -> lynxLocalDateTime.fraction)
      if (valid(standardTypeDate(lynxDuration_map))) {
        LynxDuration(standardTypeDate(lynxDuration_map))
      } else {
        throw new Exception("lynxDuration_Str is not valid")
      }
    }
  }

  def dateBetween(begin: LocalDate, end: LocalDate): Double = {
    //    Duration.between(begin, end).toMinutes * 60

    (end.toEpochDay - begin.toEpochDay) * SECOND_OF_DAY
  }


  def timeBetween(begin: LocalTime, end: LocalTime): Double = {
    (end.getHour - begin.getHour) * 3600 + (end.getMinute - begin.getMinute) * 60 +
      (end.getSecond - begin.getSecond) + (end.getNano - begin.getNano) * Math.pow(0.1, 9)
  }

  def offsetBetween(begin: ZoneOffset, end: ZoneOffset): Double = {
    end.get(ChronoField.OFFSET_SECONDS) - begin.get(ChronoField.OFFSET_SECONDS)
  }

  def between(date_1: Any, date_2: Any): LynxDuration = {

    val scale: Int = 3
    val second_Between: Double = (date_1, date_2) match {
      case (LynxDate(v_1), LynxDate(v_2)) => dateBetween(v_1, v_2)
      case (LynxDateTime(v_1), LynxDateTime(v_2)) => (LynxDateTime(v_2).epochMillis - LynxDateTime(v_1).epochMillis) * Math.pow(0.1, 3)
      case (LynxLocalDateTime(v_1), LynxLocalDateTime(v_2)) => dateBetween(v_1.toLocalDate, v_2.toLocalDate) + timeBetween(v_1.toLocalTime, v_2.toLocalTime)
      case (LynxTime(v_1), LynxTime(v_2)) => timeBetween(v_1.toLocalTime, v_2.toLocalTime) + offsetBetween(v_1.getOffset, v_2.getOffset)
      case (LynxLocalTime(v_1), LynxLocalTime(v_2)) => timeBetween(v_1, v_2)


      case _ => 0
    }
    LynxDuration(standardType(second_Between, scale))
  }
}
