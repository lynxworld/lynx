package org.grapheco.lynx

import org.junit.{Assert, Test}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

class CypherDataTypeTest extends TestBase {

  @Test
  def testLynxInteger(): Unit = {
    val r1 = runOnDemoGraph("RETURN 98988928384899 AS numLong, 938439 as numInt").records().next()
    Assert.assertEquals(98988928384899L, r1.get("numLong").get.asInstanceOf[LynxInteger].value)
    Assert.assertEquals(938439L, r1.get("numInt").get.asInstanceOf[LynxInteger].value)
  }

  @Test
  def testArrayTypeProperty(): Unit = {
    runOnDemoGraph("create (n:person{name:'xx', arr1:[1,2,3], arr2:['abc','df'], arr3:[1.5,2.0], arr4:[true,false], arr5:[]}) return n")
    val r = runOnDemoGraph("match(n:person{name:'xx'}) return n.arr1, n.arr2, n.arr3, n.arr4, n.arr5").records().next()

    Assert.assertArrayEquals(Array[Long](1,2,3), r.get("n.arr1").get.asInstanceOf[LynxList].value.map(x=>x.value.asInstanceOf[Long]).toArray)
    val arr2 = r.get("n.arr2").get.asInstanceOf[LynxList].value.map(x=>x.value.asInstanceOf[String]).toArray
    Assert.assertEquals("abc", arr2(0))
    Assert.assertEquals("df", arr2(1))
    val arr3 = r.get("n.arr3").get.asInstanceOf[LynxList].value.map(x=>x.value.asInstanceOf[Double]).toArray
    Assert.assertEquals(1.5D, arr3(0), 0)
    Assert.assertEquals(2.0D, arr3(1), 0)
    Assert.assertArrayEquals(Array[Boolean](true, false), r.get("n.arr4").get.asInstanceOf[LynxList].value.map(x=>x.value.asInstanceOf[Boolean]).toArray)
    Assert.assertEquals(0, r.get("n.arr5").get.asInstanceOf[LynxList].value.size)
  }

  @Test
  def testArrayTypeInReturn(): Unit = {
    val r = runOnDemoGraph("RETURN [1,2,3] as arr1, [true,false] as arr2").records().next()
    Assert.assertArrayEquals(Array[Long](1,2,3),
      r.get("arr1").get.asInstanceOf[LynxList].value.map(_.value.asInstanceOf[Long]).toArray)
    Assert.assertArrayEquals(Array[Boolean](true,false),
      r.get("arr2").get.asInstanceOf[LynxList].value.map(_.value.asInstanceOf[Boolean]).toArray)
  }

  @Test
  def testDateTypeProperty(): Unit = {
    runOnDemoGraph("create (n:person{name:'date1', born:date('2015-02-01')})")
    runOnDemoGraph("create (n:person{name:'date2', born:date('2015/02/02')})")

    val r1 = runOnDemoGraph("match (n:person{name:'date1'}) return n.born").records().next()
    Assert.assertEquals(LocalDate.parse("2015-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
      r1.get("n.born").get.asInstanceOf[LynxDate].value)
    val r2 = runOnDemoGraph("match (n:person{name:'date2'}) return n.born").records().next()
    Assert.assertEquals(LocalDate.parse("2015-02-02", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
      r2.get("n.born").get.asInstanceOf[LynxDate].value)
  }

  @Test
  def testDateTypeVariableArgs(): Unit = {
    runOnDemoGraph("create (n:person{name:'date1', born:date()})")
    runOnDemoGraph("create (n:person{name:'date2', born:date('2015/02/02')})")
  }

  @Test
  def testDateTimeTypeProperty(): Unit = {
    runOnDemoGraph("CREATE (Keanu:person {name:'datetime1',born:datetime('1984-10-11T12:31:14.123456789Z')})")
    runOnDemoGraph("CREATE (Keanu:person {name:'datetime2',born:datetime('1984-10-11T12:31:14.123456789+01:00')})")

    val r1 = runOnDemoGraph("match (n:person{name:'datetime1'}) return n.born").records().next()
    Assert.assertEquals(ZonedDateTime.parse("1984-10-11T12:31:14.123456789Z"),
      r1.get("n.born").get.asInstanceOf[LynxDateTime].value)
    val r2 = runOnDemoGraph("match (n:person{name:'datetime2'}) return n.born").records().next()
    Assert.assertEquals(ZonedDateTime.parse("1984-10-11T12:31:14.123456789+01:00"),
      r2.get("n.born").get.asInstanceOf[LynxDateTime].value)
  }

  @Test
  def testLocalDateTimeTypeProperty(): Unit = {
    runOnDemoGraph("CREATE (Keanu:person {name:'localdatetime1',born:localdatetime('2015-07-21T21:40:32.142')})")
    val r1 = runOnDemoGraph("match (n:person{name:'localdatetime1'}) return n.born").records().next()
    Assert.assertEquals(LocalDateTime.parse("2015-07-21T21:40:32.142"),
      r1.get("n.born").get.asInstanceOf[LynxLocalDateTime].value)
  }

  @Test
  def testLocalTimeTypeProperty(): Unit = {
    runOnDemoGraph("CREATE (Keanu:person {name:'localtime1',born: localtime('21:40:32.142')})")
    val r1 = runOnDemoGraph("match (n:person{name:'localtime1'}) return n.born").records().next()
    Assert.assertEquals(LocalTime.parse("21:40:32.142"),
      r1.get("n.born").get.asInstanceOf[LynxLocalTime].value)
  }

  @Test
  def testTimeTypeProperty(): Unit = {
    runOnDemoGraph("CREATE (Keanu:person {name:'time1',born: time('21:40:32.142+01:00')})")
    val r1 = runOnDemoGraph("match (n:person{name:'time1'}) return n.born").records().next()
    Assert.assertEquals(OffsetTime.parse("21:40:32.142+01:00"),
      r1.get("n.born").get.asInstanceOf[LynxTime].value)
  }
}
