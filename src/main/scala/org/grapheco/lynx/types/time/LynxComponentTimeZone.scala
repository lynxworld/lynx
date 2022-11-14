package org.grapheco.lynx.types.time


import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.util.LynxTemporalParseException

import java.time.{ZoneId, ZoneOffset}
import scala.util.matching.Regex

trait LynxComponentTimeZone extends LynxTemporalValue {

  //String
  var timeZone: String
  //String ±HHMM
  var offset: String
  //Integer -1080 to +1080
  var offsetMinutes: Int
  //Integer -64800 to +64800
  var offsetSeconds: Int
}

object LynxComponentTimeZone {

  def getZone(map: Map[String, Any]): ZoneId = {
    if (map.contains("timezone")) {
      getZone(map("timezone").asInstanceOf[String])
    }
    else {
      throw LynxTemporalParseException("map must contains 'timezone'")
    }
  }

  def getZone(zone: String): ZoneId = {
    if (zone == null || zone.isEmpty) {
      null
    }
    else if ("Z".equalsIgnoreCase(zone)) {
      ZoneOffset.UTC
    }
    else if (zone.startsWith("+") || zone.startsWith("-")) { // zone offset
      ZoneOffset.of(zone)
    }
    else { // zone id
      ZoneId.of(zone)
    }
  }

  def getOffset(utcStr: String): String = {
    val HH_MM: Regex = "^(.{1})([0-9]{2}):([0-9]{2})$".r
    val HHMM: Regex = "^(.{3})([0-9]{2})$".r
    val HH: Regex = "^(.{3})$".r
    utcStr match {
      case null => null
      case HHMM(hour, minute) => hour + ":" + minute
      case HH(hour) => hour + ":" + "00"
      case _ => utcStr
    }
  }

  def truncateZone(map: Map[String, Any]): ZoneId = {
    val componentsMap = map.getOrElse("mapOfComponents", null) match {
      case LynxMap(v) => v match {
        case v: Map[String, LynxString] if v.contains("timezone") =>
          return getZone(v.getOrElse("timezone", null) match {
            case LynxString(v) => v.replace(" ", "_")
            case null if map.contains("dateValue") => map.get("dateValue").orNull match {
              case LynxDateTime(v) => v.getZone.getId
              case null => "Z"
            }
            case null if map.contains("timeValue") => map.get("timeValue").orNull match {
              case LynxTime(v) => v.getOffset.getId
              case null => "Z"
            }
          })
        case _ => null
      }
      case null => null
    }

    getZone(map.getOrElse("dateValue", null) match {
      case LynxDateTime(v) => v.getZone.getId
      case null => map.getOrElse("timeValue", null) match {
        case LynxTime(v) => v.getOffset.getId
      }
    })
  }

}

