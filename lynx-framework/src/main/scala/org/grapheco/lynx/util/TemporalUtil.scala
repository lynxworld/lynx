package org.grapheco.lynx.util

import org.grapheco.lynx.util.LynxLocalDateTimeUtil.{of, parseHourMinuteSecond, parseNanoOfSecond, parseYearMonthDay, parseZone}
import org.grapheco.lynx.{LynxDate, LynxDateTime, LynxDuration, LynxException, LynxLocalDateTime, LynxLocalTime, LynxMap, LynxString, LynxTemporalValue, LynxTime, LynxValue}

import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, OffsetTime, Period, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

case class LynxTemporalParseException(msg: String) extends LynxException {
    override def getMessage: String = msg
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

  def parseYearMonthDay(map: Map[String, Any]): (Int, Int, Int) = {
    val year: Int = map.get("year").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    if (map.contains("month")) {
      assureContains(map, "year")
    }
    val month: Int = map.get("month").map(_.asInstanceOf[Long].toInt).getOrElse(1)

    if (map.contains("day")) {
      assureContains(map, "month")
    }
    val day: Int = map.get("day").map(_.asInstanceOf[Long].toInt).getOrElse(1)

    assureBetween(month, 1, 12, "month")
    assureBetween(day, 1, 31, "day")
    (year, month, day)
  }

  def parseHourMinuteSecond(map: Map[String, Any], requiredHasDay: Boolean = true): (Int, Int, Int) = {
    if (map.contains("hour") && requiredHasDay) {
      assureContains(map, "day")
    }
    val hour: Int = map.get("hour").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    if (map.contains("minute")) {
      assureContains(map, "hour")
    }
    val minute: Int = map.get("minute").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    if (map.contains("second")) {
      assureContains(map, "minute")
    }
    val second: Int = map.get("second").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    assureBetween(hour, 0, 23, "hour")
    assureBetween(minute, 0, 59, "minute")
    assureBetween(second, 0, 59, "second")
    (hour, minute, second)
  }

  def parseNanoOfSecond(map: Map[String, Any], requiredHasSecond: Boolean = true): Int = {
    if (requiredHasSecond && (map.contains("millisecond") || map.contains("microsecond") || map.contains("nanosecond"))) {
      assureContains(map, "second")
    }

    val millisecond = map.get("millisecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)
    assureBetween(millisecond, 0, 999, "millisecond value must in [0,999]")

    val microsecond = map.get("microsecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    if (map.contains("millisecond")) {
      assureBetween(microsecond, 0, 999, "microsecond")
    }
    else {
      assureBetween(millisecond, 0, 999999, "microsecond")
    }

    val nanosecond = map.get("nanosecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)

    if (map.contains("microsecond")) {
      assureBetween(nanosecond, 0, 999, "nanosecond")
    }
    else if(map.contains("millisecond")) {
      assureBetween(nanosecond, 0, 999999, "nanosecond")
    }
    else {
      assureBetween(nanosecond, 0, 999999999, "nanosecond")
    }

    val nanoOfSecond =  millisecond*1000*1000 + microsecond*1000 + nanosecond
    nanoOfSecond
  }

//
//  def parseLocalTime(map: Map[String, Any]): LocalTime = {
//    if (map.contains("hour") && map.contains("minute") && map.contains("second")) {
//      val h = map("hour").asInstanceOf[Long].toInt
//      assureBetween(h, 0, 23, "hour value must in [0,23]")
//
//      val m = map("minute").asInstanceOf[Long].toInt
//      assureBetween(m, 0, 60, "minute value must in [0,60]")
//
//      val s = map("second").asInstanceOf[Long].toInt
//      assureBetween(s, 0, 60, "second value must in [0,60]")
//
//
//      val ms = map.get("millisecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)
//      assureBetween(ms, 0, 999, "millisecond value must in [0,999]")
//
//      val us = map.get("microsecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)
//      if (map.contains("millisecond")) {
//        assureBetween(us, 0, 999, "microsecond")
//      }
//      else {
//        assureBetween(us, 0, 999999, "microsecond")
//      }
//
//      val ns = map.get("nanosecond").map(_.asInstanceOf[Long].toInt).getOrElse(0)
//
//      if (map.contains("microsecond")) {
//        assureBetween(ns, 0, 999, "nanosecond")
//      }
//      else if(map.contains("millisecond")) {
//        assureBetween(ns, 0, 999999, "nanosecond")
//      }
//      else {
//        assureBetween(ns, 0, 999999999, "nanosecond")
//      }
//
//      LocalTime.of(h, m, s, ms*1000*1000+us*1000+ns)
//    }
//    else throw LynxTemporalParseException("parse time from map: map not contains (hour, minute ,second) ")
//  }

  def parseZone(map: Map[String, Any]): ZoneId = {
    if (map.contains("timezone")) {
      parseZone(map("timezone").asInstanceOf[String])
    }
    else {
      throw LynxTemporalParseException("map must contains 'timezone'")
    }
  }

  def parseZone(zone: String): ZoneId = {
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

object LynxDateUtil extends LynxTemporalParser {
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

  def now(zoneId: ZoneId): LynxDate = {
    LynxDate(LocalDate.now(zoneId))
  }

  def of(localDate: LocalDate): LynxDate = {
    LynxDate(localDate)
  }

  def of(year: Int, month: Int, day: Int): LynxDate = {
    LynxDate(LocalDate.of(year, month, day))
  }

  def ofEpochDay(epochDay: Long): LynxDate = {
    LynxDate(LocalDate.ofEpochDay(epochDay))
  }

  override def parse(map: Map[String, Any]): LynxDate = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalDate = null
    if (map.contains("timezone")){
      if (map.size == 1) {
        v = LocalDate.now(parseZone(map("timezone").asInstanceOf[String]))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("year")) {
      val (year, month, day) = parseYearMonthDay(map)
      v = LocalDate.of(year, month, day)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (year, month, day) ")
    of(v)
  }

}


object LynxDateTimeUtil extends LynxTemporalParser{
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

  def of(zonedDateTime: ZonedDateTime): LynxDateTime = {
    LynxDateTime(zonedDateTime)
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int, timezone: String): LynxDateTime = {
    val v = ZonedDateTime.of(year, month, day, hour, minute, second, nanosecond, parseZone(timezone))
    LynxDateTime(v)
  }

  override def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: ZonedDateTime = null
    val zoneId = parseZone(map.getOrElse("timezone", "Z").asInstanceOf[String])
    if (map.size == 1 && map.contains("timezone")) {
      v = ZonedDateTime.now(zoneId)
    }
    else {
      val (year, month, day) = parseYearMonthDay(map)
      val (hour, minute, second) = parseHourMinuteSecond(map,true)
      val nanoOfSecond = parseNanoOfSecond(map)
      v = ZonedDateTime.of(year, month, day, hour, minute, second, nanoOfSecond, zoneId)
    }
    of(v)
  }
}

object LynxLocalDateTimeUtil extends LynxTemporalParser {
  def parse(localDateTimeStr: String): LynxLocalDateTime = {
    val v = LocalDateTime.parse(localDateTimeStr)
    LynxLocalDateTime(v)
  }

  def now(): LynxLocalDateTime = {
    LynxLocalDateTime(LocalDateTime.now())
  }

  def of(localDateTime: LocalDateTime): LynxLocalDateTime = {
    LynxLocalDateTime(localDateTime)
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int): LynxLocalDateTime = {
    val v = LocalDateTime.of(year, month, day, hour, minute, second, nanosecond)
    LynxLocalDateTime(v)
  }

  override def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalDateTime = null
    if (map.contains("timezone")){
      if (map.size == 1) {
        v = LocalDateTime.now(parseZone(map("timezone").asInstanceOf[String]))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("year")) {
      val (year, month, day) = parseYearMonthDay(map)
      val (hour, minute, second) = parseHourMinuteSecond(map,true)
      val nanoOfSecond = parseNanoOfSecond(map)
      v = LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (year, month, day) ")

    of(v)
  }
}

object LynxTimeUtil extends LynxTemporalParser {
  def parse(timeStr: String): LynxTime = {
    val v = OffsetTime.parse(timeStr)
    LynxTime(v)
  }

  def now(): LynxTime = {
    LynxTime(OffsetTime.now())
  }

  def of(offsetTime: OffsetTime): LynxTime = {
    LynxTime(offsetTime)
  }

  def of (hour: Int, minute: Int, second: Int, nanosOfSecond: Int, offset: ZoneOffset): LynxTime = {
    val v = OffsetTime.of(hour, minute, second, nanosOfSecond, offset)
    LynxTime(v)
  }

  def getOffset(zoneId: ZoneId): ZoneOffset = {
    if (zoneId.isInstanceOf[ZoneOffset]) zoneId.asInstanceOf[ZoneOffset]
    else zoneId.getRules.getOffset(Instant.now)
  }

  override def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: OffsetTime = null
    val zoneId = parseZone(map.getOrElse("timezone", "Z").asInstanceOf[String])
    val zoneOffset: ZoneOffset = getOffset(zoneId)

    if (map.size == 1 && map.contains("timezone")) {
      v = OffsetTime.now(zoneOffset)
    }
    else {
      val (hour, minute, second) = parseHourMinuteSecond(map,false)
      val nanoOfSecond = parseNanoOfSecond(map, true)
      v = OffsetTime.of(hour, minute, second, nanoOfSecond, zoneOffset)
    }

    of(v)
  }
}


object LynxLocalTimeUtil extends LynxTemporalParser {
  def parse(localTimeStr: String): LynxLocalTime = {
    val v = LocalTime.parse(localTimeStr)
    LynxLocalTime(v)
  }

  def now(): LynxLocalTime = {
    LynxLocalTime(LocalTime.now())
  }

  def of(localTime: LocalTime): LynxLocalTime = {
    LynxLocalTime(localTime)
  }

  def of(hour: Int, minute: Int, second: Int, nanosOfSecond: Int): LynxLocalTime = {
    val v = LocalTime.of(hour, minute, second, nanosOfSecond)
    LynxLocalTime(v)
  }

  override def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalTime = null
    if (map.contains("timezone")){
      if (map.size == 1) {
        v = LocalTime.now(parseZone(map("timezone").asInstanceOf[String]))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("hour")) {
      val (hour, minute, second) = parseHourMinuteSecond(map,false)
      val nanoOfSecond = parseNanoOfSecond(map)
      v = LocalTime.of( hour, minute, second, nanoOfSecond)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (hour, minute, second) ")

    of(v)
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
    if (map.contains("timezone")){
      throw LynxTemporalParseException("Cannot assign time zone to duration")
    }
    if (map.contains("years")){
      seconds+=map("years")*365*24*60*60
    }
    if (map.contains("months")){
      seconds+=map("months")*30*24*60*60//TODO check if neo4j does the same
    }
    if (map.contains("days")){
      seconds+=map("days")*24*60*60
    }
    if (map.contains("hours")){
      seconds+=map("hours")*60*60
    }
    if (map.contains("minutes")){
      seconds+=map("minutes")*60
    }
    if (map.contains("seconds")){
      seconds+=map("seconds")
    }
    var nanos = seconds * 1000 * 1000 * 1000
    if (map.contains("milliseconds")){
      nanos+=map("milliseconds") * 1000 * 1000
    }
    if (map.contains("microseconds")){
      nanos+=map("milliseconds") * 1000
    }
    if (map.contains("nanoseconds")){
      nanos+=map("nanoseconds")
    }
    LynxDuration(Duration.ofNanos(nanos.longValue()))
  }
}
