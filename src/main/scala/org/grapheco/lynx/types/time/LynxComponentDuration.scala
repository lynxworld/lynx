package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.util.LynxTemporalParseException

import java.time.{Duration, Period, ZoneId, ZoneOffset}
import scala.reflect.runtime.universe.This
import scala.util.matching.Regex

trait LynxComponentDuration {

  val AVG_DAYS_OF_MONTH: Double = 30.4375
  val AVG_DAYS_OF_YEAR: Double = 365.25

  //P1Y2M10DT2H30M15.03S
  var year: Int
  var month: Int
  var day: Int
  //T
  var hour: Int
  var minute: Int
  var second: Long

}

object LynxComponentDuration {

  val AVG_DAYS_OF_MONTH: Double = 30.436875
  val AVG_DAYS_OF_YEAR: Double = 365.2425
  val SECOND_OF_DAY: Double = 86400

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

  def getDuration(duration_Str: String): Duration = {
    //    val a = "PT.+".r
    //    val b = "P.".r
    //    val c = "P(.+)T(.+)".r
    //    val nanos = duration_Str match {
    //      case c(c_1, c_2) =>Period
    //      case a => Duration
    //      case b =>
    //    }
    //    val a = duration_Str.split("T", 1)
    //      Duration.ofNanos(nanos.longValue())
    Duration.ofNanos(9)
  }
}

