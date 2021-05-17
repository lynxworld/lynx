package org.grapheco.lynx

import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CallTest extends TestBase {
  @Test
  def testNonArgsCall(): Unit = {
    var rs: LynxResult = null
    rs = runOnDemoGraph("call test.authors()")
    Assert.assertEquals(Seq("name"), rs.columns)
    Assert.assertEquals(3, rs.records().size)
    Assert.assertEquals(Map("name" -> LynxValue("bluejoe")), rs.records().toSeq.apply(0))
    Assert.assertEquals(Map("name" -> LynxValue("lzx")), rs.records().toSeq.apply(1))
    Assert.assertEquals(Map("name" -> LynxValue("airzihao")), rs.records().toSeq.apply(2))
  }

  @Test
  def testWrongCall(): Unit = {
    Assert.assertThrows(classOf[UnknownProcedureException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.nonexisting()")
      }
    })

    Assert.assertThrows(classOf[WrongNumberOfArgumentsException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.authors(2)")
      }
    })
  }
  @Test
  def testSumSimple(): Unit = {
    val rs = runOnDemoGraph("match (n) return sum(n.age)").records().next()
  }
  @Test
  def testPower(): Unit = {
    val rs = runOnDemoGraph("match (n) return power(n.age, 3)").records().next()
  }
  @Test
  def testSumGroupBy(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.gender, sum(n.age)").records().next()
  }
  @Test
  def testMaxGroupBy(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.gender, max(n.age)").records().next()
  }
  @Test
  def testMinGroupBy(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.gender, min(n.age)").records().next()
  }

  @Test
  def testAbs(): Unit ={
    Assert.assertEquals(LynxDouble(10.0), runOnDemoGraph(s"return abs(${20 - 30}) as value").records().next()("value"))
  }

  @Test
  def testCeil(): Unit ={
    Assert.assertEquals(LynxDouble(1.0), runOnDemoGraph(s"return ceil(0.1) as value").records().next()("value"))
  }

  @Test
  def testFloor(): Unit ={
    Assert.assertEquals(LynxDouble(0.0), runOnDemoGraph(s"return floor(0.9) as value").records().next()("value"))
  }

  @Test
  def testRand(): Unit ={
    runOnDemoGraph(s"return rand() as value").show()
  }

  @Test
  def testRound(): Unit ={
    Assert.assertEquals(LynxInteger(3), runOnDemoGraph(s"return round(3.141592) as value").records().next()("value"))
  }
  @Test
  def testRoundWithPrecision(): Unit ={
    Assert.assertEquals(LynxDouble(3.142), runOnDemoGraph(s"return round(3.141592, 3) as value").records().next()("value"))
  }

  @Test
  def testSign(): Unit ={
    Assert.assertEquals(LynxDouble(-1.0), runOnDemoGraph(s"return sign(-17) as value").records().next()("value"))
    Assert.assertEquals(LynxDouble(1.0), runOnDemoGraph(s"return sign(17) as value").records().next()("value"))
    Assert.assertEquals(LynxDouble(0.0), runOnDemoGraph(s"return sign(0) as value").records().next()("value"))
  }

  @Test
  def testE(): Unit ={
    Assert.assertEquals(LynxDouble(Math.E), runOnDemoGraph(s"return e() as value").records().next()("value"))
  }

  @Test
  def testExp(): Unit ={
    Assert.assertEquals(LynxDouble(7.38905609893065), runOnDemoGraph(s"return exp(2) as value").records().next()("value"))
  }

  @Test
  def testLog(): Unit ={
    Assert.assertEquals(LynxDouble(3.295836866004329), runOnDemoGraph(s"return log(27) as value").records().next()("value"))
  }

  @Test
  def testLog10(): Unit ={
    Assert.assertEquals(LynxDouble(1.4313637641589874), runOnDemoGraph(s"return log10(27) as value").records().next()("value"))
  }

  @Test
  def testSqrt(): Unit ={
    Assert.assertEquals(LynxDouble(16.0), runOnDemoGraph(s"return sqrt(256) as value").records().next()("value"))
  }

  @Test
  def testAcos(): Unit ={
    Assert.assertEquals(LynxDouble(1.0471975511965979), runOnDemoGraph(s"return acos(0.5) as value").records().next()("value"))
  }

  @Test
  def testAsin(): Unit ={
    Assert.assertEquals(LynxDouble(0.5235987755982989), runOnDemoGraph(s"return asin(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan(): Unit ={
    Assert.assertEquals(LynxDouble(0.4636476090008061), runOnDemoGraph(s"return atan(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan2(): Unit ={
    Assert.assertEquals(LynxDouble(0.6947382761967033), runOnDemoGraph(s"return atan2(0.5, 0.6) as value").records().next()("value"))
  }

  @Test
  def testCos(): Unit ={
    Assert.assertEquals(LynxDouble(0.8775825618903728), runOnDemoGraph(s"return cos(0.5) as value").records().next()("value"))
  }

  @Test
  def testCot(): Unit ={
    Assert.assertEquals(LynxDouble(1.830487721712452), runOnDemoGraph(s"return cot(0.5) as value").records().next()("value"))
  }

  @Test
  def testDegrees(): Unit ={
    Assert.assertEquals(LynxDouble(179.9998479605043), runOnDemoGraph(s"return degrees(3.14159) as value").records().next()("value"))
  }

  @Test
  def testHaversin(): Unit ={
    Assert.assertEquals(LynxDouble(0.06120871905481362), runOnDemoGraph("return haversin(0.5) as value").records().next()("value"))
  }

  @Test
  def testPi(): Unit ={
    Assert.assertEquals(LynxDouble(Math.PI), runOnDemoGraph(s"return pi() as value").records().next()("value"))
  }

  @Test
  def testRadians(): Unit ={
    Assert.assertEquals(LynxDouble(3.141592653589793), runOnDemoGraph(s"return radians(180) as value").records().next()("value"))
  }

  @Test
  def testSin(): Unit ={
    Assert.assertEquals(LynxDouble(0.479425538604203), runOnDemoGraph(s"return sin(0.5) as value").records().next()("value"))
  }

  @Test
  def testTan(): Unit ={
    Assert.assertEquals(LynxDouble(0.5463024898437905), runOnDemoGraph(s"return tan(0.5) as value").records().next()("value"))
  }
}
