package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.time.LynxDuration
import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-02 15:30
 */
class J_Temporal_Duration extends TestBase {


  @Test
  def creatingDurationFromDurationComponents(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration({ days: 14, hours:16, minutes: 12 }),
        |duration({ months: 5, days: 1.5 }),
        |duration({ months: 0.75 }),
        |duration({ weeks: 2.5 }),
        |duration({ minutes: 1.5, seconds: 1, milliseconds: 123, microseconds: 456, nanoseconds: 789 }),
        |duration({ minutes: 1.5, seconds: 1, nanoseconds: 123456789 })
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(6, records.length)
    Assert.assertEquals(LynxDuration.parse("P14DT16H12M").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P5M1DT12H").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P22DT19H51M49.5S").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P17DT12H").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT1M31.123456789S").toString, records(4)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT1M31.123456789S").toString, records(5)("aDuration").toString)
  }

  @Test
  def creatingDurationFromString(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration("P14DT16H12M"),
        |duration("P5M1.5D"),
        |duration("P0.75M"),
        |duration("PT0.75M"),
        |duration("P2012-02-02T14:37:21.545")
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    Assert.assertEquals(LynxDuration.parse("P14DT16H12M").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P5M1DT12H").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P22DT19H51M49.5S").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT45S").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P2012Y2M2DT14H37M21.545S").toString, records(4)("aDuration").toString)
  }

  @Test
  def durationBetween(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration.between(date("1984-10-11"), date("1985-11-25")),
        |duration.between(date("1985-11-25"), date("1984-10-11")),
        |duration.between(date("1984-10-11"), datetime("1984-10-12T21:40:32.142+0100")),
        |duration.between(date("2015-06-24"), localtime("14:30")),
        |duration.between(localtime("14:30"), time("16:30+0100")),
        |duration.between(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
        |duration.between(datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm' }), datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/London' }))
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(7, records.length)
    Assert.assertEquals(LynxDuration.parse("P1Y1M14D").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P-1Y-1M-14D").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P1DT21H40M32.142S").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT14H30M").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT2H").toString, records(4)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P1YT4M50S").toString, records(5)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT1H").toString, records(6)("aDuration").toString)
  }

  @Test
  def durationInMonths(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration.inMonths(date("1984-10-11"), date("1985-11-25")),
        |duration.inMonths(date("1985-11-25"), date("1984-10-11")),
        |duration.inMonths(date("1984-10-11"), datetime("1984-10-12T21:40:32.142+0100")),
        |duration.inMonths(date("2015-06-24"), localtime("14:30")),
        |duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
        |duration.inMonths(datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm' }), datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/London' }))
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(6, records.length)
    Assert.assertEquals(LynxDuration.parse("P1Y1M").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P-1Y-1M").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT0S").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT0S").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P1Y").toString, records(4)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT0S").toString, records(5)("aDuration").toString)
  }

  @Test
  def durationInDays(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration.inDays(date("1984-10-11"), date("1985-11-25")),
        |duration.inDays(date("1985-11-25"), date("1984-10-11")),
        |duration.inDays(date("1984-10-11"), datetime("1984-10-12T21:40:32.142+0100")),
        |duration.inDays(date("2015-06-24"), localtime("14:30")),
        |duration.inDays(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
        |duration.inDays(datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm' }), datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/London' }))
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(6, records.length)
    Assert.assertEquals(LynxDuration.parse("P410D").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P-410D").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P1D").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT0S").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("P366D").toString, records(4)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT0S").toString, records(5)("aDuration").toString)
  }

  @Test
  def durationInSeconds(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [
        |duration.inSeconds(date("1984-10-11"), date("1984-10-12")),
        |duration.inSeconds(date("1984-10-12"), date("1984-10-11")),
        |duration.inSeconds(date("1984-10-11"), datetime("1984-10-12T01:00:32.142+0100")),
        |duration.inSeconds(date("2015-06-24"), localtime("14:30")),
        |duration.inSeconds(datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm' }), datetime({ year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/London' }))
        |] AS aDuration
        |RETURN aDuration
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    Assert.assertEquals(LynxDuration.parse("PT24H").toString, records(0)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT-24H").toString, records(1)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT25H32.142S").toString, records(2)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT14H30M").toString, records(3)("aDuration").toString)
    Assert.assertEquals(LynxDuration.parse("PT1H").toString, records(4)("aDuration").toString)
  }
}