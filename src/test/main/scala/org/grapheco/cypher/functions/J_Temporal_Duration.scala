package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
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
    Assert.assertEquals("P14DT16H12M", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P5M1DT12H", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P22DT19H51M49.5S", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P17DT12H", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT1M31.123456789S", records(4)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT1M31.123456789S", records(5)("aDuration").asInstanceOf[LynxValue].value)
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
    Assert.assertEquals("P14DT16H12M", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P5M1DT12H", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P22DT19H51M49.5S", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT45S", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P2012Y2M2DT14H37M21.545S", records(4)("aDuration").asInstanceOf[LynxValue].value)
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
    Assert.assertEquals("P1Y1M14D", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P-1Y-1M-14D", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P1DT21H40M32.142S", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT14H30M", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT2H", records(4)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P1YT4M50S", records(5)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT1H", records(6)("aDuration").asInstanceOf[LynxValue].value)
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
    Assert.assertEquals("P1Y1M", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P-1Y-1M", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT0S", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT0S", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P1Y", records(4)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT0S", records(5)("aDuration").asInstanceOf[LynxValue].value)
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
    Assert.assertEquals("P410D", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P-410D", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P1D", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT0S", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("P366D", records(4)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT0S", records(5)("aDuration").asInstanceOf[LynxValue].value)
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
    Assert.assertEquals("PT24H", records(0)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT-24H", records(1)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT25H32.142S", records(2)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT14H30M", records(3)("aDuration").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT1H", records(4)("aDuration").asInstanceOf[LynxValue].value)
  }
}




