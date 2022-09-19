package org.grapheco.lynx.util

import org.apache.commons.lang3.time.DateUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.util.LynxLocalDateTimeUtil.{of, parseHourMinuteSecond, parseNanoOfSecond, parseYearMonthDay, parseZone}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.time.{LynxDate, LynxDateTime, LynxDuration, LynxLocalDateTime, LynxLocalTime, LynxTemporalValue, LynxTime}
import org.grapheco.lynx.{LynxException, types}

import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, OffsetTime, Period, ZoneId, ZoneOffset, ZonedDateTime}
import java.util
import java.util.{Calendar, GregorianCalendar}
import scala.util.matching.Regex

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
    val year: Int = map.get("year").map(
      _ match {
        case v: LynxInteger => v.value.toInt
        case v: Long => v.toInt
      }
    ).getOrElse(0)

    if (map.contains("month")) {
      assureContains(map, "year")
    }
    val month: Int = map.get("month").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    if (map.contains("day")) {
      assureContains(map, "month")
    }
    val day: Int = map.get("day").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    assureBetween(month, 1, 12, "month")
    assureBetween(day, 1, 31, "day")
    (year, month, day)
  }

  def parseYearWeekDay(map: Map[String, Any]): (Int, Int, Int) = {
    val calendar = new GregorianCalendar()
    calendar.set(Calendar.WEEK_OF_YEAR, 1)
    calendar.set(Calendar.DAY_OF_WEEK, 1)
    val year: Int = map.get("year").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("week")) {
      assureContains(map, "year")
    }
    val week: Int = map.get("week").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    if (map.contains("dayOfWeek")) {
      assureContains(map, "week")
    }
    val dayOfWeek: Int = map.get("dayOfWeek").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    assureBetween(week, 1, 53, "week")
    assureBetween(dayOfWeek, 1, 7, "dayOfWeek")

    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.WEEK_OF_YEAR, week)
    calendar.set(Calendar.DAY_OF_WEEK, dayOfWeek)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH) + 1)
  }

  def parseYearQuarterDay(map: Map[String, Any]): (Int, Int, Int) = {
    val calendar = new GregorianCalendar()
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    val year: Int = map.get("year").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("quarter")) {
      assureContains(map, "year")
    }
    val quarter: Int = map.get("quarter").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    if (map.contains("dayOfQuarter")) {
      assureContains(map, "quarter")
    }
    val dayOfQuarter: Int = map.get("dayOfQuarter").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    assureBetween(quarter, 1, 4, "quarter")
    assureBetween(dayOfQuarter, 1, 92, "dayOfQuarter")

    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, quarter * 3 - 2)
    calendar.set(Calendar.DAY_OF_MONTH, dayOfQuarter)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH))
  }

  def truncateDate(map: Map[String, Any]): (Int, Int, Int) = {
    //TODO
//    var (year, month, day) = map.get("dateValue").getOrElse(0) match {
//      case v: LynxDate => (v.localDate.getYear, v.localDate.getMonthValue, v.localDate.getDayOfMonth)
//      case v: LynxDateTime => (v.zonedDateTime.getYear, v.zonedDateTime.getMonthValue, v.zonedDateTime.getDayOfMonth)
//      case v: LynxLocalDateTime => (v.localDateTime.getYear, v.localDateTime.getMonthValue, v.localDateTime.getDayOfMonth)
//    }
//    map match {
//      case v.size == 2 => DateUtils.truncate (map.get ("dateValue").getOrElse (0), Calendar.YEAR)
//      case v == 3 => DateUtils.truncate(map.get("dateValue").getOrElse(0), Calendar.YEAR)
//    }
//    DateUtils.truncate(map.get("dateValue").getOrElse(0), Calendar.YEAR)

        (1990,1, 1)
  }

  def parseToDate(map: Map[String, Any]): (Int, Int, Int) = {
    var (year, month, day) = map.get("date").getOrElse(0) match {
      case v: LynxDate => (v.localDate.getYear, v.localDate.getMonthValue, v.localDate.getDayOfMonth)
      case v: LynxDateTime => (v.zonedDateTime.getYear, v.zonedDateTime.getMonthValue, v.zonedDateTime.getDayOfMonth)
      case v: LynxLocalDateTime => (v.localDateTime.getYear, v.localDateTime.getMonthValue, v.localDateTime.getDayOfMonth)
    }
    if (map.contains("day")) {
      assureContains(map, "date")
      day = map.get("day").map(_ match {
        case v: LynxInteger => v.value.toInt
        case v: Long => v.toInt
      }
      ).getOrElse(0)

    }
    (year, month, day)
  }

  def parseYearOrdinalDay(map: Map[String, Any]): (Int, Int, Int) = {
    val calendar = new GregorianCalendar()

    val year: Int = map.get("year").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("ordinalDay")) {
      assureContains(map, "year")
    }
    val ordinalDay: Int = map.get("ordinalDay").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(1)

    assureBetween(ordinalDay, 1, 366, "ordinalDay")

    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.DAY_OF_YEAR, ordinalDay)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
  }

  def parseHourMinuteSecond(map: Map[String, Any], requiredHasDay: Boolean = true): (Int, Int, Int) = {
    if (map.contains("hour") && requiredHasDay) {
      assureContains(map, "day")
    }
    val hour: Int = map.get("hour").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("minute")) {
      assureContains(map, "hour")
    }
    val minute: Int = map.get("minute").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("second")) {
      assureContains(map, "minute")
    }
    val second: Int = map.get("second").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    assureBetween(hour, 0, 23, "hour")
    assureBetween(minute, 0, 59, "minute")
    assureBetween(second, 0, 59, "second")
    (hour, minute, second)
  }

  def parseNanoOfSecond(map: Map[String, Any], requiredHasSecond: Boolean = true): Int = {
    if (requiredHasSecond && (map.contains("millisecond") || map.contains("microsecond") || map.contains("nanosecond"))) {
      assureContains(map, "second")
    }

    val millisecond = map.get("millisecond").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)
    assureBetween(millisecond, 0, 999, "millisecond value must in [0,999]")

    val microsecond = map.get("microsecond").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("millisecond")) {
      assureBetween(microsecond, 0, 999, "microsecond")
    }
    else {
      assureBetween(millisecond, 0, 999999, "microsecond")
    }

    val nanosecond = map.get("nanosecond").map(_ match {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }
    ).getOrElse(0)

    if (map.contains("microsecond")) {
      assureBetween(nanosecond, 0, 999, "nanosecond")
    }
    else if (map.contains("millisecond")) {
      assureBetween(nanosecond, 0, 999999, "nanosecond")
    }
    else {
      assureBetween(nanosecond, 0, 999999999, "nanosecond")
    }

    val nanoOfSecond = millisecond * 1000 * 1000 + microsecond * 1000 + nanosecond
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
    val double_Hyphen_Flag: Regex = "([0-9]{4})-([0-9]{2})-([0-9]{2})".r
    val double_Slash_Flag: Regex = "([0-9]{4})/([0-9]{2})/([0-9]{2})".r
    val yearMonthDay_Flag: Regex = "([0-9]{4})([0-9]{2})([0-9]{2})".r
    val yearMonth_Flag: Regex = "([0-9]{4})([0-9]{2})".r
    val single_Hyphen_Flag: Regex = "([0-9]{4})-([0-9]{2})".r
    val weekDay_Flag: Regex = "([0-9]{4})-W([0-9]{2})-+([1-7]{1})".r
    val yearOrdinalDay_Flag: Regex = "([0-9]{4})([0-9]{3})".r
    val year_Flag: Regex = "([0-9]{4})".r

    val (year, month, day) = dateStr match {
      case double_Hyphen_Flag(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case double_Slash_Flag(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case yearMonthDay_Flag(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case single_Hyphen_Flag(year, month) => (year.toInt, month.toInt, 1)
      case yearMonth_Flag(year, month) => (year.toInt, month.toInt, 1)
      case year_Flag(year) => (year.toInt, 1, 1)
      case weekDay_Flag(year, week, day) => parseYearWeekDay(Map("year" -> year.toLong, "week" -> week.toLong, "dayOfWeek" -> day.toLong))
      case yearOrdinalDay_Flag(year, day) => parseYearOrdinalDay(Map("year" -> year.toLong, "ordinalDay" -> day.toLong))
    }
    LynxDateUtil.of(year, month, day)
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
    if (map.contains("timezone")) {
      if (map.size == 1) {
        v = LocalDate.now(parseZone(map("timezone").asInstanceOf[String].replace(" ", "_")))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("unitStr")) {
      val (year, month, day) = truncateDate(map)
      v = LocalDate.of(year, month, day)
    }
    else if (map.contains("date")) {
      val (year, month, day) = parseToDate(map)
      v = LocalDate.of(year, month, day)
    }
    else if (map.contains("ordinalDay")) {
      val (year, month, day) = parseYearOrdinalDay(map)
      v = LocalDate.of(year, month, day)
    }
    else if (map.contains("quarter")) {
      val (year, month, day) = parseYearQuarterDay(map)
      v = LocalDate.of(year, month, day)
    }
    else if (map.contains("week")) {
      val (year, month, day) = parseYearWeekDay(map)
      v = LocalDate.of(year, month, day)
    }
    else if (map.contains("year")) {
      val (year, month, day) = parseYearMonthDay(map)
      v = LocalDate.of(year, month, day)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (year, month, day) ")
    of(v)
  }

}


object LynxDateTimeUtil extends LynxTemporalParser {
  def parse(zonedDateTimeStr: String): LynxDateTime = {
    try {
      val v = ZonedDateTime.parse(zonedDateTimeStr)
      LynxDateTime(v)
    } catch {
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
      val (hour, minute, second) = parseHourMinuteSecond(map, true)
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
    if (map.contains("timezone")) {
      if (map.size == 1) {
        v = LocalDateTime.now(parseZone(map("timezone").asInstanceOf[String]))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("year")) {
      val (year, month, day) = parseYearMonthDay(map)
      val (hour, minute, second) = parseHourMinuteSecond(map, true)
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

  def of(hour: Int, minute: Int, second: Int, nanosOfSecond: Int, offset: ZoneOffset): LynxTime = {
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
      val (hour, minute, second) = parseHourMinuteSecond(map, false)
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
    if (map.contains("timezone")) {
      if (map.size == 1) {
        v = LocalTime.now(parseZone(map("timezone").asInstanceOf[String]))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("hour")) {
      val (hour, minute, second) = parseHourMinuteSecond(map, false)
      val nanoOfSecond = parseNanoOfSecond(map)
      v = LocalTime.of(hour, minute, second, nanoOfSecond)
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

  //TODO
  //  def doMethod(methodName: String, methodParam: List[Map[String, Any]]) {
  //    val methodParam_size = methodParam.size
  //    var method_param_type: Class[_] = new Class[_]
  //    var method_param_value: Object = new Object
  //    if (methodParam_size > 0) {
  //      method_param_type = new Class[](methodParam_size)
  //      method_param_value = new Array[AnyRef](methodParam_size)
  //      for (i <- 0 until methodParam_size) {
  //        method_param_type(i) = methodParam.get(i).get("argType").asInstanceOf[Class[_]]
  //        method_param_value(i) = methodParam.get(i).get("argValue")
  //      }
  //    }
  //    return this.getClass.getMethod(methodName, method_param_type).invoke(this, method_param_value)
  //  }

}


