package org.grapheco.lynx.util

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.time.{LynxDateTime, LynxDuration, LynxTemporalValue}
import org.grapheco.lynx.LynxException
import java.time.Duration

import scala.util.matching.Regex

case class LynxTemporalParseException(msg: String) extends LynxException {
  override def getMessage: String = msg

  def timestamp: LynxInteger = LynxInteger(LynxDateTime.now().epochMillis)
}

trait LynxTemporalParser {
  def parse(str: String): LynxTemporalValue

  def parse(map: Map[String, Any]): LynxTemporalValue

  def parse(v: LynxValue): LynxTemporalValue = {
    v match {
      case x: LynxString => parse(v.value.asInstanceOf[String])
      case x: LynxMap => parse(x.value.mapValues(_.value))
    }
  }
}

object LynxTemporalParser {

  def assureBetween(v: Long, min: Long, max: Long, valueName: String): Unit = {
    if (v < min || v > max) {
      throw LynxTemporalParseException(s"$valueName value must in range[$min, $max]")
    }
  }

  def assureBetween(v: Int, min: Int, max: Int, valueName: String): Unit = {
    if (v < min || v > max) {
      throw LynxTemporalParseException(s"$valueName value must in range[$min, $max]")
    }
  }

  def assureContains(map: Map[String, Any], key: String): Unit = {
    if (!map.contains(key)) {
      throw LynxTemporalParseException(s"$key must be specified")
    }
  }

  def splitDateTime(str: String): Map[String, String] = {
    val splitDateTime: Regex = "^(.{4,12})T(.{0,40})$".r
    val splitDateTimeWithZone_1: Regex = "^(.{4,12})T(.{0,40})(Z{1})$".r
    val splitDateTimeWithZone_2: Regex = "^(.{4,12})T(.{0,40})(\\+.{0,40})$".r
    val splitDateTimeWithZone_3: Regex = "^(.{4,12})T(.{0,40})(\\-.{0,40})$".r
    val splitDateTimeWithZone_4: Regex = "^(.{4,12})T(.{0,40})\\[(.{0,40}).$".r
    val splitDateTimeWithZone_5: Regex = "^(.{4,12})T(.{0,40})(\\+.{0,40})\\[(.{0,40}).$".r
    val splitDateTimeWithZone_6: Regex = "^(.{4,12})T(.{0,40})(\\-.{0,40})\\[(.{0,40}).$".r
    val splitTimeWithOffset_1: Regex = "^(.{0,40})(\\+.{0,40})$".r
    val splitTimeWithOffset_2: Regex = "^(.{0,40})(\\-.{0,40})$".r
    val splitTimeWithOffset_3: Regex = "^(.{0,40})(Z{1})$".r
    str match {
      case splitDateTimeWithZone_6(dateStr, timeStr, offsetStr, zoneStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "offsetStr" -> offsetStr, "zoneStr" -> zoneStr)
      case splitDateTimeWithZone_5(dateStr, timeStr, offsetStr, zoneStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "offsetStr" -> offsetStr, "zoneStr" -> zoneStr)
      case splitDateTimeWithZone_4(dateStr, timeStr, zoneStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "zoneStr" -> zoneStr)
      case splitDateTimeWithZone_1(dateStr, timeStr, zoneStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "zoneStr" -> zoneStr)
      case splitDateTimeWithZone_2(dateStr, timeStr, offsetStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "offsetStr" -> offsetStr)
      case splitDateTimeWithZone_3(dateStr, timeStr, offsetStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr, "offsetStr" -> offsetStr)
      case splitTimeWithOffset_1(timeStr, offsetStr) => Map("timeStr" -> timeStr, "offsetStr" -> offsetStr)
      case splitTimeWithOffset_2(timeStr, offsetStr) => Map("timeStr" -> timeStr, "offsetStr" -> offsetStr)
      case splitTimeWithOffset_3(timeStr, offsetStr) => Map("timeStr" -> timeStr, "offsetStr" -> offsetStr)
      case splitDateTime(dateStr, timeStr) => Map("dateStr" -> dateStr, "timeStr" -> timeStr)
      case _ => throw new Exception("can not split Date and Time")
    }
  }
}

object LynxDurationUtil {
  def parse(durationStr: String): LynxDuration = {
    val v = Duration.parse(durationStr)
    LynxDuration(v)
  }

  def parse(map: Map[String, Double]): LynxDuration = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var seconds: Double = 0
    if (map.contains("timezone")) {
      throw LynxTemporalParseException("Cannot assign time zone to duration")
    }
    if (map.contains("years")) {
      seconds += map("years") * 365 * 24 * 60 * 60
    }
    if (map.contains("months")) {
      seconds += map("months") * 30 * 24 * 60 * 60 //TODO check if neo4j does the same
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
    var nanos = seconds * 1000 * 1000 * 1000
    if (map.contains("milliseconds")) {
      nanos += map("milliseconds") * 1000 * 1000
    }
    if (map.contains("microseconds")) {
      nanos += map("milliseconds") * 1000
    }
    if (map.contains("nanoseconds")) {
      nanos += map("nanoseconds")
    }
    LynxDuration(Duration.ofNanos(nanos.longValue()))
  }


}
