package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{HasProperty, LynxPropertyKey}
import org.grapheco.lynx.util.LynxTemporalParseException

import java.time.{Duration, LocalDate, LocalTime, Period, ZoneId, ZoneOffset}
import scala.reflect.runtime.universe.This
import scala.util.matching.Regex

trait LynxComponentDuration {

  val AVG_DAYS_OF_MONTH: Double = 30.4375
  val AVG_DAYS_OF_YEAR: Double = 365.25

  //P1Y2M10DT2H30M15.03S
  var years: Int
  var quarters: Int
  var quartersOfYear: Int
  var months: Int
  var monthsOfYear: Int
  var monthsOfQuarter: Int
  var weeks: Int
  var days: Int
  var daysOfWeek: Int
  //T
  var hours: Long
  var minutes: Long
  var seconds: Long
  var milliseconds: Long
  var microseconds: Long
  var nanoseconds: Long
  var minutesOfHour: Int
  var secondsOfMinute: Int
  var millisecondsOfSecond: Long
  var microsecondsOfSecond: Long
  var nanosecondsOfSecond: Long


}

object LynxComponentDuration {

  val AVG_DAYS_OF_MONTH: Double = 30.436875
  val AVG_DAYS_OF_YEAR: Double = 365.2425
  val SECOND_OF_DAY: Double = 86400

  //  def getDurationString(lynxDuration: LynxDuration): String = {
  //    var year, month, day, hour, minute, second = ""
  //    if (lynxDuration.year != 0) {
  //      year = lynxDuration.year + "Y"
  //    }
  //    if (lynxDuration.month != 0) {
  //      month = lynxDuration.month + "M"
  //    }
  //    if (lynxDuration.day != 0) {
  //      day = lynxDuration.day + "D"
  //    }
  //    if (lynxDuration.hour != 0) {
  //      hour = lynxDuration.hour + "H"
  //    }
  //    if (lynxDuration.minute != 0) {
  //      minute = lynxDuration.minute + "M"
  //    }
  //    if (lynxDuration.second != 0) {
  //      second = lynxDuration.second + "S"
  //    }
  //    "P" + year + month + day + "T" + hour + minute + second
  //  }

  def getDuration(map: Map[String, Double]): Duration = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var seconds: Double = 0
    if (map.contains("timezone")) {
      throw LynxTemporalParseException("Cannot assign time zone to duration")
    }
    if (map.contains("years")) {
      seconds += map("years") * AVG_DAYS_OF_YEAR * 24 * 60 * 60
    }
    if (map.contains("months")) {
      seconds += map("months") * AVG_DAYS_OF_MONTH * 24 * 60 * 60 //TODO check if neo4j does the same
    }
    if (map.contains("days")) {
      seconds += map("days") * 24 * 60 * 60
    }
    if (map.contains("hours")) {
      seconds += map("hours") * 60 * 60
    }
    if (map.contains("minutes")) {
      seconds += map("minutes") * 60
    }
    if (map.contains("seconds")) {
      seconds += map("seconds")
    }
    var nanos = seconds * Math.pow(10, 9)
    if (map.contains("milliseconds")) {
      nanos += map("milliseconds") * Math.pow(10, 6)
    }
    if (map.contains("microseconds")) {
      nanos += map("milliseconds") * Math.pow(10, 3)
    }
    if (map.contains("nanoseconds")) {
      nanos += map("nanoseconds")
    }
    Duration.ofNanos(nanos.longValue())
  }

  def between(date_1: Any, date_2: Any): (LocalDate, LocalTime, ZoneOffset, LocalDate, LocalTime, ZoneOffset) = {
    val (begin_Date, begin_Time, begin_Zone): (LocalDate, LocalTime, ZoneOffset) = date_1 match {
      case LynxDate(v) => (v, LocalTime.parse("00:00:00"), null)
      case LynxDateTime(v) => (v.toLocalDate, v.toLocalTime, v.getOffset)
      case LynxLocalDateTime(v) => (v.toLocalDate, v.toLocalTime, null)
      case LynxTime(v) => (null, v.toLocalTime, v.getOffset)
      case LynxLocalTime(v) => (null, v, null)
      case _ => (null, null, null)
    }
    val (end_Date, end_Time, end_Zone): (LocalDate, LocalTime, ZoneOffset) = date_2 match {
      case LynxDate(v) => (v, LocalTime.parse("00:00:00"), null)
      case LynxDateTime(v) => (v.toLocalDate, v.toLocalTime, v.getOffset)
      case LynxLocalDateTime(v) => (v.toLocalDate, v.toLocalTime, null)
      case LynxTime(v) => (null, v.toLocalTime, v.getOffset)
      case LynxLocalTime(v) => (null, v, null)
      case _ => (null, null, null)
    }
    (begin_Date, begin_Time, begin_Zone, end_Date, end_Time, end_Zone)
  }
}

