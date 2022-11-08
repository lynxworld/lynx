package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.time.{LynxDateTime, LynxLocalDateTime}
import org.junit.{Assert, Test}

import java.time.ZonedDateTime

class TemporalValues extends TestBase {

  /**
   * 3.1
   */
  @Test
  def temporalInstantsEx1(): Unit = {
    val records = runOnDemoGraph("RETURN datetime('2015-06-24T12:50:35.556+0100') AS theDateTime")
      .records().map(f => f("theDateTime").asInstanceOf[LynxDateTime]).toArray

    /*how to create Lynx temporal object by 2015-06-24T12:50:35.556+0100 */
    Assert.assertEquals(LynxDateTime.parse("2015-06-24T12:50:35.556+0100"), records(0))
  }

  @Test
  def temporalInstantsEx2(): Unit = {
    val records = runOnDemoGraph("RETURN localdatetime('2015185T19:32:24') AS theLocalDateTime")
      .records().map(f => f("theLocalDateTime").asInstanceOf[LynxLocalDateTime]).toArray
    Assert.assertEquals(1, records.length)
  }

  @Test
  def temporalInstantsEx3(): Unit = {
    val records = runOnDemoGraph("RETURN date('+2015-W13-4') AS theDate")
      .records().map(f => f("theDate")).toArray
    Assert.assertEquals(1, records.length)
  }

  @Test
  def temporalInstantsEx4(): Unit = {
    val records = runOnDemoGraph("RETURN time('125035.556+0100') AS theTime")
      .records().map(f => f("theTime")).toArray
    Assert.assertEquals(1, records.length)
  }

  @Test
  def temporalInstantsEx5(): Unit = {
    val records = runOnDemoGraph("RETURN localtime('12:50:35.556') AS theLocalTime")
      .records().map(f => f("theLocalTime")).toArray
    Assert.assertEquals(1, records.length)
  }

  /**
   * 3.2
   */
  @Test
  def AccessComponentOfTimeEx1(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH date({ year:1984, month:10, day:11 }) AS d
        |RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.dayOfWeek, d.dayOfQuarter
        |""".stripMargin)
      .records().map(f => Map(
      "d.year" -> f("d.year").value, "d.quarter" -> f("d.quarter").value, "d.month" -> f("d.month").value,
      "d.week" -> f("d.week").value, "d.weekYear" -> f("d.weekYear").value, "d.day" -> f("d.day").value,
      "d.ordinalDay" -> f("d.ordinalDay").value, "d.dayOfWeek" -> f("d.dayOfWeek").value, "d.dayOfQuarter" -> f("d.dayOfQuarter").value
    )).toArray
    Assert.assertEquals(1, records.length)
  }


  @Test
  def AccessComponentOfTimeEx2(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH datetime({ year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'Europe/Stockholm' }) AS d
        |RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.dayOfWeek, d.dayOfQuarter
        |""".stripMargin)
      .records().map(f => Map(
      "d.year" -> f("d.year").value, "d.quarter" -> f("d.quarter").value, "d.month" -> f("d.month").value,
      "d.week" -> f("d.week").value, "d.weekYear" -> f("d.weekYear").value, "d.day" -> f("d.day").value,
      "d.ordinalDay" -> f("d.ordinalDay").value, "d.dayOfWeek" -> f("d.dayOfWeek").value, "d.dayOfQuarter" -> f("d.dayOfQuarter").value
    ))
    Assert.assertEquals(1, records.length)
  }


  @Test
  def AccessComponentOfTimeEx3(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH datetime({ year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'Europe/Stockholm' }) AS d
        |RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
        |""".stripMargin)
      .records().map(f => Map(
      "d.hour" -> f("d.hour").value, "d.minute" -> f("d.minute").value, "d.second" -> f("d.second").value,
      "d.millisecond" -> f("d.millisecond").value, "d.microsecond" -> f("d.microsecond").value, "d.nanosecond" -> f("d.nanosecond").value,
    )).toArray

    val expectResult = Map(
      "d.hour" -> 12l, "d.minute" -> 31l, "d.second" -> 14l,
      "d.millisecond" -> 645l, "d.microsecond" -> 645876l, "d.nanosecond" -> 645876123l,
    )
    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  @Test
  def AccessComponentOfTimeEx4(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH datetime({ year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'Europe/Stockholm' }) AS d
        |RETURN d.timezone, d.offset, d.offsetMinutes, d.epochSeconds, d.epochMillis
        |""".stripMargin)
      .records().map(f => Map(
      "d.timezone" -> f("d.timezone").value.toString, "d.offset" -> f("d.offset").value.toString,
      "d.offsetMinutes" -> f("d.offsetMinutes").value.toString, "d.epochSeconds" -> f("d.epochSeconds").value.toString,
      "d.epochMillis" -> f("d.epochMillis").value.toString
    )).toArray

    val expectResult = Map(
      "d.timezone" -> "Europe/Stockholm", "d.offset" -> "+01:00",
      "d.offsetMinutes" -> "60", "d.epochSeconds" -> "469020674",
      "d.epochMillis" -> "469020674645"
    )

    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  /**
   * 4.1
   */
  @Test
  def createDurationByStrEx1(): Unit = {
    val records = runOnDemoGraph("RETURN duration('P14DT16H12M') AS theDuration").records().map(f => f("theDuration").value).toArray
    Assert.assertEquals("P14DT16H12M", records(0))
  }

  @Test
  def createDurationByStrEx2(): Unit = {
    val records = runOnDemoGraph("RETURN duration('P5M1.5D') AS theDuration").records().map(f => f("theDuration").value).toArray
    Assert.assertEquals("P5M1DT12H", records(0))
  }

  @Test
  def createDurationByStrEx3(): Unit = {
    val records = runOnDemoGraph("RETURN duration('PT0.75M') AS theDuration").records().map(f => f("theDuration").value).toArray
    Assert.assertEquals("PT45S", records(0))
  }

  @Test
  def createDurationByStrEx4(): Unit = {
    val records = runOnDemoGraph("RETURN duration('P2.5W') AS theDuration").records().map(f => f("theDuration").value).toArray
    Assert.assertEquals("P17DT12H", records(0))
  }

  /**
   * 4.2
   */

  @Test
  def accessDurationEx1(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH duration({ years: 1, months:5, days: 111, minutes: 42 }) AS d
        |RETURN d.years, d.quarters, d.quartersOfYear, d.months, d.monthsOfYear, d.monthsOfQuarter
        |""".stripMargin)
      .records().map(f => Map(
      "d.year" -> f("d.year").value, "d.quarters" -> f("d.quarters").value,
      "d.quartersOfYear" -> f("d.quartersOfYear").value, "d.months" -> f("d.months").value,
      "d.monthsOfYear" -> f("d.monthOfYear").value, "d.monthsOfQuarter" -> f("d.monthsOfQuarter").value
    )).toArray

    val expectResult = Map(
      "d.year" -> 1l, "d.quarters" -> 5l, "d.quartersOfYear" -> 1l,
      "d.months" -> 17l, "d.monthsOfYear" -> 5l, "d.monthsOfQuarter" -> 2l
    )

    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  @Test
  def accessDurationEx2(): Unit = {
    val records = runOnDemoGraph("WITH duration({ months:5, days: 25, hours: 1 }) AS d\nRETURN d.weeks, d.days, d.daysOfWeek")
      .records().map(f => Map(
      "d.weeks" -> f("d.weeks").value,
      "d.days" -> f("d.days").value,
      "d.daysOfWeek" -> f("d.daysOfWeek").value
    )).toArray
    val expectResult = Map("d.weeks" -> 3l, "d.days" -> 25l, "d.daysOfWeek" -> 4l)

    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  @Test
  def accessDurationEx3(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH duration({ years: 1, months:1, days:1, hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111 }) AS d
        |RETURN d.hours, d.minutes, d.seconds, d.milliseconds, d.microseconds, d.nanoseconds
        |""".stripMargin)
      .records().map(f => Map(
      "d.hours" -> f("d.hours").value, "d.minutes" -> f("d.minutes").value,
      "d.seconds" -> f("d.seconds").value, "d.milliseconds" -> f("d.milliseconds").value,
      "d.microseconds" -> f("d.microseconds").value, "d.nanoseconds" -> f("d.nanoseconds").value
    )).toArray

    val expectResult = Map(
      "d.hours" -> "1", "d.minutes" -> "61l", "d.seconds" -> "3661",
      "d.milliseconds" -> "3661111", "d.microseconds" -> "3661111111",
      "d.nanoseconds" -> "3661111111111"
    )
    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  @Test
  def accessDurationEx4(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH duration({ years: 1, months:1, days:1, hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111 }) AS d
        |RETURN d.minutesOfHour, d.secondsOfMinute, d.millisecondsOfSecond, d.microsecondsOfSecond, d.nanosecondsOfSecond
        |""".stripMargin)
      .records().map(f => Map(
      "d.minutesOfHour" -> f("d.minutesOfHour").value,
      "d.secondsOfMinutes" -> f("d.secondsOfMinutes").value,
      "d.millisecondsOfSecond" -> f("d.millisecondsOfSecond").value,
      "d.microsecondsOfSecond" -> f("d.microsecondsOfSecond").value,
      "d.nanosecondsOfSecond" -> f("d.nanosecondsOfSecond").value
    )).toArray
    val expectResult = Map(
      "d.minutesOfHour" -> 1l, "d.secondsOfMinutes" -> 1l, "d.millisecondsOfSecond" -> 111l,
      "d.microsecondsOfSecond" -> 111111l, "d.nanosecondsOfSecond" -> 111111111l
    )
    expectResult.foreach(f => {
      Assert.assertEquals(f._2, records(0)(f._1))
    })
  }

  /**
   * 5.0 examples
   */
  @Test
  def examples(): Unit = {
    val cypher: List[Map[String, String]] = List(
      Map("cypher" -> "RETURN duration({ days: 1, hours: 12 }) AS theDuration",
        "col" -> "theDuration", "result" -> "P1DT12H"),
      Map("cypher" -> "RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS theDuration",
        "col" -> "theDuration", "result" -> "P30Y8M13D"),
      Map("cypher" -> "RETURN duration.inDays(date('2014-10-11'), date('2015-08-06')) AS theDuration",
        "col" -> "theDuration", "result" -> "P299D"),
      Map("cypher" -> "RETURN date.truncate('week', date(), { dayOfWeek: 4 }) AS thursday",
        "col" -> "thursday", "result" -> "2021-09-30"),
      Map("cypher" -> "RETURN date.truncate('month', date()+ duration('P2M'))- duration('P1D') AS lastDay",
        "col" -> "lastDay", "result" -> "2021-10-31"),
      Map("cypher" -> "RETURN time('13:42:19')+ duration({ days: 1, hours: 12 }) AS theTime",
        "col" -> "theTime", "result" -> "01:42:19Z"),
      Map("cypher" -> "RETURN duration({ days: 2, hours: 7 })+ duration({ months: 1, hours: 18 }) AS theDuration",
        "col" -> "theDuration", "result" -> "P1M2DT25H"),
      Map("cypher" -> "RETURN duration({ hours: 5, minutes: 21 })* 14 AS theDuration",
        "col" -> "theDuration", "result" -> "PT74H54M"),
      Map("cypher" -> "RETURN duration({ hours: 3, minutes: 16 })/ 2 AS theDuration",
        "col" -> "theDuration", "result" -> "PT1H38M"),
      Map("cypher" ->
        """
          |WITH datetime('2015-07-21T21:40:32.142+0100') AS date1, datetime('2015-07-21T17:12:56.333+0100') AS date2
          |RETURN
          |CASE
          |WHEN date1 < date2
          |THEN date1 + duration("P1D")> date2
          |ELSE date2 + duration("P1D")> date1 END AS lessThanOneDayApart
          |""".stripMargin,
        "col" -> "lessThanOneDayApart", "result" -> "true"),
      Map("cypher" -> "RETURN [\"Jan\", \"Feb\", \"Mar\", \"Apr\", \"May\", \"Jun\", \"Jul\", \"Aug\", \"Sep\", \"Oct\", \"Nov\", \"Dec\"][date().month-1] AS month",
        "col" -> "month", "result" -> "Sep")
    )

    var flag = 0
    cypher.foreach(e => {
      try {
        val records = runOnDemoGraph(e("cypher")).records().map(f => f(e("col")).value.toString).toArray
        Assert.assertEquals(e("result"), records(0))
      } catch {
        case ex: AssertionError => {
          logger.error("AssertError for cypher:" + e("cypher"))
          logger.error(ex.toString + "\n\n")
          flag += 1
        }
        case ex: Exception => {
          logger.error("ScalaError for cypher:" + e("cypher"))
          logger.error(ex.toString + "\n\n")
          flag += 1
        }
      }
    })
    if (flag > 0) {
      throw new Exception("The example find error cypher " + flag + "/" + cypher.length)
    }
  }
}
