package org.grapheco.lynx.types.time


import org.grapheco.lynx.util.{LynxTemporalParseException, LynxTemporalParser}

import java.time.{ZoneId, ZoneOffset}
import java.util.TimeZone
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


}

