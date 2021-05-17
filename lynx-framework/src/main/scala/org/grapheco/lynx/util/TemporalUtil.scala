package org.grapheco.lynx.util

import org.grapheco.lynx.{LynxDate, LynxDateTime, LynxLocalDateTime, LynxLocalTime, LynxTime}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter


object LynxDateUtil {
  def parse(dateStr: String): LynxDate = {
    var v: LocalDate = null
    if (dateStr.contains('-')) {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    }
    else if (dateStr.contains('/')) {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy/MM/dd"))
    }
    else {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"))
    }

    LynxDate(v)
  }

  def now(): LynxDate = {
    LynxDate(LocalDate.now())
  }

  def of(year: Int, month: Int, day: Int): LynxDate = {
    LynxDate(LocalDate.of(year, month, day))
  }

  def ofEpochDay(epochDay: Long): LynxDate = {
    LynxDate(LocalDate.ofEpochDay(epochDay))
  }
}


object LynxDateTimeUtil {
  def parse(zonedDateTimeStr: String): LynxDateTime = {
    try{
      val v = ZonedDateTime.parse(zonedDateTimeStr)
      LynxDateTime(v)
    }catch  {
      case _ => throw new Exception("DateTimeParseException")
    }
  }

  def now(): LynxDateTime = {
    LynxDateTime(ZonedDateTime.now())
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int, timezone: String): LynxDateTime = {
    val v = ZonedDateTime.of(year, month, day, hour, minute, second, nanosecond, parseZone(timezone))
    LynxDateTime(v)
  }

  def parseZone(zone: String): ZoneId = {
    if (zone == null || zone.isEmpty) {
      null
    }
    else if("Z".equalsIgnoreCase(zone)) {
      ZoneOffset.UTC
    }
    else if (zone.startsWith("+") || zone.startsWith("-")) {  // zone offset
      ZoneOffset.of(zone)
    }
    else {  // zone id
      ZoneId.of(zone)
    }
  }


}

object LynxLocalDateTimeUtil {
  def parse(localDateTimeStr: String): LynxLocalDateTime = {
    val v = LocalDateTime.parse(localDateTimeStr)
    LynxLocalDateTime(v)
  }

  def now(): LynxLocalDateTime = {
    LynxLocalDateTime(LocalDateTime.now())
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int): LynxLocalDateTime = {
    val v = LocalDateTime.of(year, month, day, hour, minute, second, nanosecond)
    LynxLocalDateTime(v)
  }
}

object LynxLocalTimeUtil {
  def parse(localTimeStr: String): LynxLocalTime = {
    val v = LocalTime.parse(localTimeStr)
    LynxLocalTime(v)
  }

  def now(): LynxLocalTime = {
    LynxLocalTime(LocalTime.now())
  }

  def of(hour: Int, minute: Int, second: Int, nanosOfSecond: Int): LynxLocalTime = {
    val v = LocalTime.of(hour, minute, second, nanosOfSecond)
    LynxLocalTime(v)
  }
}

object LynxTimeUtil {
  def parse(timeStr: String): LynxTime = {
    val v = OffsetTime.parse(timeStr)
    LynxTime(v)
  }

  def now(): LynxTime = {
    LynxTime(OffsetTime.now())
  }

  def of (hour: Int, minute: Int, second: Int, nanosOfSecond: Int, offset: ZoneOffset): LynxTime = {
    val v = OffsetTime.of(hour, minute, second, nanosOfSecond, offset)
    LynxTime(v)
  }
}