package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.time.{LynxDate, LynxLocalDateTime}
import org.junit.{Assert, Test}

import java.time.{LocalDate, LocalDateTime}


class Operators extends TestBase {

  /**
   * 2.1
   */
  @Test
  def distinctOP(): Unit = {
    val records = runOnDemoGraph(
      """
        |CREATE (a:Person { name: 'Anne', eyeColor: 'blue' }),(b:Person { name: 'Bill', eyeColor: 'brown' }),(c:Person { name: 'Carol', eyeColor: 'blue' })
        |WITH [a, b, c] AS ps
        |UNWIND ps AS p
        |RETURN DISTINCT p.eyeColor
        |""".stripMargin)
      .records().map(f => f("p.eyeColor").value).toArray.sortBy(r => r.asInstanceOf[String])
    val expectResult = Array("blue", "brown").sorted
    compareArray(expectResult, records)
  }

  /**
   * 3.1
   */
  @Test
  def dotOp(): Unit = {
    val records = runOnDemoGraph(
      """
        |CREATE (a:Person { name: 'Jane', livesIn: 'London' }),(b:Person { name: 'Tom', livesIn: 'Copenhagen' })
        |WITH a, b
        |MATCH (p:Person)
        |RETURN p.name
        |""".stripMargin)
      .records().map(f => f("p.name").value).toArray.sortBy(r => r.asInstanceOf[String])
    val expectResult = Array("Jane", "Tom").sorted
    compareArray(expectResult, records)
  }


  /**
   * 3.2
   */
  @Test
  def dynamicPropertyOp(): Unit = {
    val records = runOnDemoGraph(
      """
        |CREATE (a:Restaurant { name: 'Hungry Jo', rating_hygiene: 10, rating_food: 7 }),(b:Restaurant { name: 'Buttercup Tea Rooms', rating_hygiene: 5, rating_food: 6 }),(c1:Category { name: 'hygiene' }),(c2:Category { name: 'food' })
        |WITH a, b, c1, c2
        |MATCH (restaurant:Restaurant),(category:Category)
        |WHERE restaurant["rating_" + category.name]> 6
        |RETURN DISTINCT restaurant.name
        |""".stripMargin)
      .records().map(f => f("restaurant.name").value).toArray
    val expectResult = Array("Hungry Jo")
    compareArray(expectResult, records)
  }

  /**
   * 3.3
   */
  @Test
  def replacePropertyByEqualOp(): Unit = {
    val records = runOnDemoGraph(
      """
        |CREATE (a:Person { name: 'Jane', age: 20 })
        |WITH a
        |MATCH (p:Person { name: 'Jane' })
        |SET p = { name: 'Ellen', livesIn: 'London' }
        |RETURN p.name, p.age, p.livesIn
        |""".stripMargin)
      .records().map(f => Map("p.name" -> f("p.name").value, "p.age" -> f("p.age").value, "p.livesIn" -> f("p.livesIn").value)).toArray
    Assert.assertEquals(Map("p.name" -> "Ellen", "p.age" -> null, "p.livesln" -> "London"), records(0))
  }

  /**
   * 3.4
   */
  @Test
  def mutatingPropertyOp(): Unit = {
    val records = runOnDemoGraph(
      """
        |CREATE (a:Person { name: 'Jane', age: 20 })
        |WITH a
        |MATCH (p:Person { name: 'Jane' })
        |SET p += { name: 'Ellen', livesIn: 'London' }
        |RETURN p.name, p.age, p.livesIn
        |""".stripMargin)
      .records().map(f => Map("p.name" -> f("p.name").value, "p.age" -> f("p.age").value, "p.livesln" -> f("p.livesln").value)).toArray
    Assert.assertEquals(Map("p.name" -> "Ellen", "p.age" -> 20, "p.livesln" -> "London"), records(0))
  }


  /**
   * 4.1
   */
  @Test
  def powOp(): Unit = {
    val records = runOnDemoGraph("WITH 2 AS number, 3 AS exponent\nRETURN number ^ exponent AS result")
      .records().map(f => f("result").value).toArray
    Assert.assertEquals(8.0, records(0))
  }

  /**
   * 4.2
   */
  @Test
  def minusOp(): Unit = {
    val records = runOnDemoGraph("WITH -3 AS a, 4 AS b\nRETURN b - a AS result").records().map(f => f("result").value).toArray
    Assert.assertEquals(7l, records(0))
  }


  /**
   * 5.2
   */
  @Test
  def compareNumberOp(): Unit = {
    val records = runOnDemoGraph("WITH 4 AS one, 3 AS two\nRETURN one > two AS result").records().map(f => f("result").value).toArray
    Assert.assertEquals(true, records(0))
  }

  /**
   * 5.3
   */
  @Test
  def startWithOp(): Unit = {
    val records = runOnDemoGraph("WITH ['John', 'Mark', 'Jonathan', 'Bill'] AS somenames\nUNWIND somenames AS names\nWITH names AS candidate\nWHERE candidate STARTS WITH 'Jo'\nRETURN candidate")
      .records().map(f => f("candidate").value).toArray.sortBy(r => r.asInstanceOf[String])
    val expectResult = Array("John", "Jonathan").sorted
    compareArray(expectResult, records)
  }


  /**
   * 6.1
   */
  @Test
  def boolOp(): Unit = {
    val records = runOnDemoGraph("WITH [2, 4, 7, 9, 12] AS numberlist\nUNWIND numberlist AS number\nWITH number\nWHERE number = 4 OR (number > 6 AND number < 10)\nRETURN number")
      .records()
      .map(f => f("number").value)
      .toArray
      .sortBy(r => r.asInstanceOf[Long])
    val expectResult = Array(4l, 7l, 9l).sorted
    compareArray(expectResult, records)
  }


  /**
   * 7.1
   */
  @Test
  def stringOp(): Unit = {
    val records = runOnDemoGraph("WITH ['mouse', 'chair', 'door', 'house'] AS wordlist\nUNWIND wordlist AS word\nWITH word\nWHERE word =~ '.*ous.*'\nRETURN word")
      .records()
      .map(f => f("word").value)
      .toArray
      .sortBy(r => r.asInstanceOf[String])
    val expectResult = Array("mouse", "house").sorted
    compareArray(expectResult, records)
  }


  /**
   * 8.1
   */
  @Test
  def addAndSubTimeOpEx1(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH localdatetime({ year:1984, month:10, day:11, hour:12, minute:31, second:14 }) AS aDateTime, duration({ years: 12, nanoseconds: 2 }) AS aDuration
        |RETURN aDateTime + aDuration, aDateTime - aDuration
        |""".stripMargin)
      .records()
      .map(f => Map("aDateTime + aDuration" -> f("aDateTime + aDuration").asInstanceOf[LynxLocalDateTime], "aDateTime - aDuration" -> f("aDateTime - aDuration").asInstanceOf[LynxLocalDateTime]))
      .toArray
    val expectResult = Map("aDateTime + aDuration" -> LynxLocalDateTime(LocalDateTime.parse("1996-10-11T12:31:14.000000002")), "aDateTime - aDuration" -> LynxLocalDateTime(LocalDateTime.parse("1972-10-11T12:31:13.999999998")))
    Assert.assertEquals(expectResult, records(0))
  }


  @Test
  def addAndSubTimeOpEx2(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH date({ year:1984, month:10, day:11 }) AS aDate, duration({ years: 12, nanoseconds: 2 }) AS aDuration
        |RETURN aDate + aDuration, aDate - aDuration
        |""".stripMargin)
      .records()
      .map(f => Map("aDate + aDuration" -> f("aDate + aDuration").asInstanceOf[LynxDate], "aDate - aDuration" -> f("aDate - aDuration").asInstanceOf[LynxDate]))
      .toArray
    val expectResult = Map("aDate + aDuration" -> LynxDate(LocalDate.parse("1996-10-11")), "aDate - aDuration" -> LynxDate(LocalDate.parse("1972-10-11")))
    Assert.assertEquals(expectResult, records(0))
  }

  @Test
  def addAndSubTimeOpEx3(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN (date("2011-01-31")+ duration("P1M"))+ duration("P12M") AS date1,
        | date("2011-01-31")+(duration("P1M")+ duration("P12M")) AS date2
        |""".stripMargin)
      .records()
      .map(f => Map("date1" -> f("date1").asInstanceOf[LynxDate], "date2" -> f("date2").asInstanceOf[LynxDate]))
      .toArray
    val expectResult = Map("date1" -> LynxDate(LocalDate.parse("2012-02-28")), "date2" -> LynxDate(LocalDate.parse("2012-02-29")))
    Assert.assertEquals(expectResult, records(0))
  }


  /**
   * 8.2
   */
  @Test
  def addAndSubTimeOpEx4(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH duration({ years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1 }) AS duration1, duration({ months:1, days: -14, hours: 16, minutes: -12, seconds: 70 }) AS duration2
        |RETURN duration1, duration2, duration1 + duration2, duration1 - duration2
        |""".stripMargin)
      .records()
      .map(f => Map("duration1" -> f("duration1").value, "duration2" -> f("duration2").value, "duration1 + duration2" -> f("duration1 + duration2"), "duration1 - duration2" -> f("duration1 - duration2")))
      .toArray
    val expectResult = Map("duration1" -> LynxValue("P12Y5M14DT16H13M10.000000001S"), "duration2" -> LynxValue("P1M-14DT15H49M10S"), "duration1 + duration2" -> LynxValue("P12Y6MT32H2M20.000000001S"), "duration1 - duration2" -> LynxValue("P12Y4M28DT24M0.000000001S"))
    Assert.assertEquals(expectResult, records(0))
  }

  /**
   * 8.3
   */
  @Test
  def multiAndDivTime(): Unit = {
    val records = runOnDemoGraph("WITH duration({ days: 14, minutes: 12, seconds: 70, nanoseconds: 1 }) AS aDuration\nRETURN aDuration, aDuration * 2, aDuration / 3")
      .records()
      .map(f => Map("aDuration" -> f("aDuration").value, "aDuration * 2" -> f("aDuration * 2").value, "aDuration / 3" -> f("aDuration / 3").value))
      .toArray

    val expectResult = Map("aDuration" -> "P14DT13M10.000000001S", "aDuration * 2" -> "P28DT26M20.000000002S", "aDuration / 3" -> "P4DT16H4M23.333333333S")
    Assert.assertEquals(expectResult, records)
  }


  /**
   * 9.1
   */
  @Test
  def mapStaticOp(): Unit = {
    val records = runOnDemoGraph("WITH { person: { name: 'Anne', age: 25 }} AS p\nRETURN p.person.name")
      .records().map(f => f("p.person.name").value).toArray
    Assert.assertEquals("Anne", records(0))
  }

  /**
   * 9.2
   */
  @Test
  def mapDynamicOp(): Unit = {
    val myKey = "\"name\""
    val records = runOnDemoGraph(s"WITH { name: 'Anne', age: 25 } AS a\nRETURN a[${myKey}] AS result").records().map(f => f("result")).toArray
    Assert.assertEquals(LynxValue("Anne"), records(0))
  }


  /**
   * 10.1
   */
  @Test
  def concatList(): Unit = {
    val records = runOnDemoGraph("RETURN [1,2,3,4,5]+[6,7] AS myList").records().map(f => f("myList").asInstanceOf[LynxList]).toArray
    val expectResult = Array(1, 2, 3, 4, 5, 6, 7).map(f => LynxValue(f))
    compareArray(expectResult, records(0).value.toArray)
  }


  /**
   * 10.2
   */
  @Test
  def checkNumInList(): Unit = {
    val records = runOnDemoGraph("WITH [2, 3, 4, 5] AS numberlist\nUNWIND numberlist AS number\nWITH number\nWHERE number IN [2, 3, 8]\nRETURN number")
      .records().map(f => f("number").value).toArray
    val expectResult = Array(2l, 3l)
    compareArray(expectResult, records)
  }

  /**
   * 10.3
   */
  @Test
  def checkListInList(): Unit = {
    var records = runOnDemoGraph("RETURN [2, 1] IN [1,[2, 1], 3] AS inList")
      .records().map(f => f("inList").value).toArray
    Assert.assertEquals(true, records(0))

    records = runOnDemoGraph("RETURN [1, 2] IN [1, 2] AS inList")
      .records().map(f => f("inList").value).toArray
    Assert.assertEquals(false, records(0))
  }

  /**
   * 10.4
   */
  @Test
  def accessElemInList(): Unit = {
    val records = runOnDemoGraph("WITH ['Anne', 'John', 'Bill', 'Diane', 'Eve'] AS names\nRETURN names[1..3] AS result")
      .records().map(f => f("result").asInstanceOf[LynxList]).toArray
    val expectResult = Array("John", "Bill").map(f => LynxValue(f))
    compareArray(expectResult, records(0).value.toArray)
  }

  /**
   * 10.5
   */

  @Test
  def dynamicAccessElemInList(): Unit = {
    val myIndex = 1
    val records = runOnDemoGraph(s"WITH ['Anne', 'John', 'Bill', 'Diane', 'Eve'] AS names\nRETURN names[${myIndex}] AS result")
      .records().map(f => f("result")).toArray
    Assert.assertEquals("John", records(0).value)
  }


  /**
   * 10.6
   */
  @Test
  def checkInNestList(): Unit = {
    val records = runOnDemoGraph("WITH [[1, 2, 3]] AS l\nRETURN 3 IN l[0] AS result")
      .records().map(f => f("result")).toArray
    Assert.assertEquals(true, records(0).value)
  }

  /**
   * compare expect Result with actual Result
   *
   * @param expectResult
   * @param records
   * @tparam A
   */
  def compareArray[A](expectResult: Array[A], records: Array[Any]): Unit = {
    Assert.assertEquals(expectResult.length, records.length)
    for (i <- 0 to records.length - 1) {
      Assert.assertEquals(expectResult(i), records(i))
    }
  }
}
