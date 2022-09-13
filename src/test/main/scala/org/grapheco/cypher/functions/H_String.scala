package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.NodeInput
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-01 10:02
 */
class H_String extends TestBase {

  @Test
  def left(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN left('hello', 3)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hel", records(0)("left('hello', 3)").asInstanceOf[LynxValue].value)
  }

  @Test
  def ltrim(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN lTrim('   hello')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hello", records(0)("lTrim('   hello')").asInstanceOf[LynxValue].value)
  }

  @Test
  def replace(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN replace("hello", "l", "w")
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hewwo", records(0)("replace(\"hello\", \"l\", \"w\")").asInstanceOf[LynxValue].value)
  }

  @Test
  def reverse(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN reverse('anagram')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("margana", records(0)("reverse('anagram')").asInstanceOf[LynxValue].value)
  }

  @Test
  def right(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN right('hello', 3)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("llo", records(0)("right('hello', 3)").asInstanceOf[LynxValue].value)
  }

  @Test
  def rtrim(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN rTrim('hello   ')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hello", records(0)("rTrim('hello   ')").asInstanceOf[LynxValue].value)
  }

  @Test
  def split(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN split('one,two', ',')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("one"),LynxString("two")), records(0)("split('one,two', ',')").asInstanceOf[LynxValue].value)
  }

  @Test
  def substring(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN substring('hello', 1, 3), substring('hello', 2)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("ell", records(0)("substring('hello', 1, 3)").asInstanceOf[LynxValue].value)
    Assert.assertEquals("llo", records(0)("substring('hello', 2)").asInstanceOf[LynxValue].value)
  }

  @Test
  def toLower(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN toLower('HELLO')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hello", records(0)("toLower('HELLO')").asInstanceOf[LynxValue].value)
  }

  @Test
  def toString_Function(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN toString(11.5), toString('already a string'), toString(TRUE ), toString(date({ year:1984, month:10, day:11 })) AS dateString, toString(datetime({ year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 341, timezone: 'Europe/Stockholm' })) AS datetimeString, toString(duration({ minutes: 12, seconds: -60 })) AS durationString
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("11.5", records(0)("toString(11.5)").asInstanceOf[LynxValue].value)
    Assert.assertEquals("already a string", records(0)("toString('already a string')").asInstanceOf[LynxValue].value)
    Assert.assertEquals("true", records(0)("toString(TRUE )").asInstanceOf[LynxValue].value)
    Assert.assertEquals("1984-10-11", records(0)("dateString").asInstanceOf[LynxValue].value)
    Assert.assertEquals("1984-10-11T12:31:14.341+01:00[Europe/Stockholm]", records(0)("datetimeString").asInstanceOf[LynxValue].value)
    Assert.assertEquals("PT11M", records(0)("durationString").asInstanceOf[LynxValue].value)
  }

  @Test
  def toUpper(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN toUpper('hello')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("HELLO", records(0)("toUpper('hello')").asInstanceOf[LynxValue].value)
  }

  @Test
  def trim(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN trim('   hello   ')
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("hello", records(0)("trim('   hello   ')").asInstanceOf[LynxValue].value)
  }
}




