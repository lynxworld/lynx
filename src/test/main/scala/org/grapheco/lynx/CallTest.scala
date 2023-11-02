package org.grapheco.lynx

import org.grapheco.lynx.procedure.UnknownProcedureException
import org.grapheco.lynx.types.time.LynxDuration
//import org.grapheco.lynx.util.LynxDurationUtil
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CallTest extends TestBase {

  runOnDemoGraph(
    """
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c{name:"Bob", age: 10, gender:"male"}),
      |(d{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)
  @Test
  def testNonArgsCall(): Unit = {
    var rs: LynxResult = null
    rs = runOnDemoGraph("call test.authors()")
    Assert.assertEquals(Seq("name"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(Map("name" ->LynxList(List(LynxString("bluejoe"), LynxString("lzx"), LynxString("airzihao")))), rs.records().toSeq.apply(0))
  }

  @Test
  def testFullTextIndexQuery(): Unit = {
    var rs = runOnDemoGraph("call db.index.fulltext.queryNodes(\"111\", \"222\")")
    Assert.assertEquals(1, rs.records().size)
  }

  @Test
  def testFullTextIndexCreate(): Unit = {
    var rs = runOnDemoGraph("call db.index.fulltext.createNodeIndex(\"111\", [\"222\"],  [\"333\"])")
    Assert.assertEquals(1, rs.records().size)


  }

  @Test
  def testWrongCall(): Unit = {
    Assert.assertThrows(classOf[UnknownProcedureException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.nonexisting()")
      }
    })

    Assert.assertThrows(classOf[UnknownProcedureException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.authors(2)")
      }
    })
  }

  @Test
  def testCountSimple(): Unit = {
    var rs = runOnDemoGraph("match (n) return count(n)").records().next()("count(n)")
    Assert.assertEquals(LynxInteger(4), rs)
  }

  @Test
  def testSumSimple(): Unit = {
    val rs = runOnDemoGraph("match (n) return sum(n.age)").records().next()("sum(n.age)")
    Assert.assertEquals(LynxFloat(90), rs)
  }

  @Test
  def testSumEmpty(): Unit = {
    val rs = runOnDemoGraph("match (n:notexists) return sum(n.age)").records().next()("sum(n.age)")
    Assert.assertEquals(LynxFloat(0), rs)
  }

  @Test
  def testAvg(): Unit = {
    val rs = runOnDemoGraph("match (n) return avg(n.age)").records().next()("avg(n.age)")
    Assert.assertEquals(LynxFloat(90/4.0), rs)
  }

  @Test
  def testNoneAvg(): Unit = {
    val rs = runOnDemoGraph("match (n:notexists) return avg(n.age)").records().next()("avg(n.age)")
    Assert.assertEquals(LynxNull, rs)
  }

  @Test
  def testDuration(): Unit = {
    runOnDemoGraph("CREATE (:profile {works: duration('P18DT16H12M'), history: duration({years: 10.2, months: 5, days: 14, hours:16, minutes: 12})})")
    runOnDemoGraph("CREATE (:profile {works: duration('P10DT16H12M'), history: duration({seconds: 1, milliseconds: 123, microseconds: 456, nanoseconds: 789})})")
    val rs = runOnDemoGraph("match (n:profile) return avg(n.works), sum(n.history)").records().next()
    Assert.assertEquals(LynxDuration.parse("PT352H12M"), rs("avg(n.works)"))
    Assert.assertEquals(LynxDuration.parse("PT93304H12M1.123123725S"), rs("sum(n.history)"))
  }

  @Test
  def testScalarSize(): Unit = {
    val rs = runOnDemoGraph("RETURN size(['Alice', 'Bob'])").records().next().get(0).get
    Assert.assertEquals(LynxInteger(2), rs)
  }

  @Test
  def testStringSize(): Unit = {
    val rs = runOnDemoGraph("RETURN size('Alice')").records().next().get(0).get
    Assert.assertEquals(LynxInteger(5), rs)
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
    Assert.assertEquals(LynxInteger(10), runOnDemoGraph(s"return abs(${20 - 30}) as value").records().next()("value"))
    Assert.assertEquals(LynxFloat(9.8), runOnDemoGraph(s"return abs(${20.2 - 30}) as value").records().next()("value"))
  }

  @Test
  def testCeil(): Unit ={
    Assert.assertEquals(LynxFloat(1.0), runOnDemoGraph(s"return ceil(0.1) as value").records().next()("value"))
  }

  @Test
  def testFloor(): Unit ={
    Assert.assertEquals(LynxFloat(0.0), runOnDemoGraph(s"return floor(0.9) as value").records().next()("value"))
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
    Assert.assertEquals(LynxFloat(3.142), runOnDemoGraph(s"return round(3.141592, 3) as value").records().next()("value"))
  }

  @Test
  def testSign(): Unit ={
    Assert.assertEquals(LynxFloat(-1.0), runOnDemoGraph(s"return sign(-17) as value").records().next()("value"))
    Assert.assertEquals(LynxFloat(1.0), runOnDemoGraph(s"return sign(17) as value").records().next()("value"))
    Assert.assertEquals(LynxFloat(0.0), runOnDemoGraph(s"return sign(0) as value").records().next()("value"))
  }

  @Test
  def testE(): Unit ={
    Assert.assertEquals(LynxFloat(Math.E), runOnDemoGraph(s"return e() as value").records().next()("value"))
  }

  @Test
  def testExp(): Unit ={
    Assert.assertEquals(LynxFloat(7.38905609893065), runOnDemoGraph(s"return exp(2) as value").records().next()("value"))
  }

  @Test
  def testLog(): Unit ={
    Assert.assertEquals(LynxFloat(3.295836866004329), runOnDemoGraph(s"return log(27) as value").records().next()("value"))
  }

  @Test
  def testLog10(): Unit ={
    Assert.assertEquals(LynxFloat(1.4313637641589874), runOnDemoGraph(s"return log10(27) as value").records().next()("value"))
  }

  @Test
  def testSqrt(): Unit ={
    Assert.assertEquals(LynxFloat(16.0), runOnDemoGraph(s"return sqrt(256) as value").records().next()("value"))
  }

  @Test
  def testAcos(): Unit ={
    Assert.assertEquals(LynxFloat(1.0471975511965979), runOnDemoGraph(s"return acos(0.5) as value").records().next()("value"))
  }

  @Test
  def testAsin(): Unit ={
    Assert.assertEquals(LynxFloat(0.5235987755982989), runOnDemoGraph(s"return asin(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan(): Unit ={
    Assert.assertEquals(LynxFloat(0.4636476090008061), runOnDemoGraph(s"return atan(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan2(): Unit ={
    Assert.assertEquals(LynxFloat(0.6947382761967033), runOnDemoGraph(s"return atan2(0.5, 0.6) as value").records().next()("value"))
  }

  @Test
  def testCos(): Unit ={
    Assert.assertEquals(LynxFloat(0.8775825618903728), runOnDemoGraph(s"return cos(0.5) as value").records().next()("value"))
  }

  @Test
  def testCot(): Unit ={
    Assert.assertEquals(LynxFloat(1.830487721712452), runOnDemoGraph(s"return cot(0.5) as value").records().next()("value"))
  }

  @Test
  def testHaversin(): Unit ={
    Assert.assertEquals(LynxFloat(0.06120871905481362), runOnDemoGraph("return haversin(0.5) as value").records().next()("value"))
  }

  @Test
  def testPi(): Unit ={
    Assert.assertEquals(LynxFloat(Math.PI), runOnDemoGraph(s"return pi() as value").records().next()("value"))
  }

  @Test
  def testRadians(): Unit ={
    Assert.assertEquals(LynxFloat(3.141592653589793), runOnDemoGraph(s"return radians(180) as value").records().next()("value"))
  }

  @Test
  def testSin(): Unit ={
    Assert.assertEquals(LynxFloat(0.479425538604203), runOnDemoGraph(s"return sin(0.5) as value").records().next()("value"))
  }

  @Test
  def testTan(): Unit ={
    Assert.assertEquals(LynxFloat(0.5463024898437905), runOnDemoGraph(s"return tan(0.5) as value").records().next()("value"))
  }

  // String Functions
  @Test
  def testLeft(): Unit ={
    Assert.assertEquals(LynxString("hel"), runOnDemoGraph("return left('hello', 3) as value").records().next()("value"))
  }

  @Test
  def testRight(): Unit ={
    Assert.assertEquals(LynxString("llo"), runOnDemoGraph("return right('hello', 3) as value").records().next()("value"))
  }

  @Test
  def testTrim(): Unit ={
    Assert.assertEquals(LynxString("hello"), runOnDemoGraph("return ltrim('    hello') as value").records().next()("value"))
    Assert.assertEquals(LynxString("hello"), runOnDemoGraph("return rtrim('hello    ') as value").records().next()("value"))
    Assert.assertEquals(LynxString("hello"), runOnDemoGraph("return trim('    hello  ') as value").records().next()("value"))
  }

  @Test
  def testReplace(): Unit ={
    Assert.assertEquals(LynxString("hezzo"), runOnDemoGraph("return replace('hello', 'l', 'z') as value").records().next()("value"))
  }

  @Test
  def testReverse(): Unit ={
    Assert.assertEquals(LynxString("olleh"), runOnDemoGraph("return reverse('hello') as value").records().next()("value"))
  }

  @Test
  def testSplit(): Unit ={
    Assert.assertEquals(LynxList(List(LynxString("one"), LynxString("two"))), runOnDemoGraph("return split('one,two', ',') as value").records().next()("value"))
  }

  @Test
  def testLowerAndUpper(): Unit ={
    Assert.assertEquals(LynxString("hello"), runOnDemoGraph("return toLower('HELLO') as value").records().next()("value"))
    Assert.assertEquals(LynxString("HELLO"), runOnDemoGraph("return toUpper('hello') as value").records().next()("value"))
  }

  @Test
  def testToString(): Unit ={
    Assert.assertEquals(LynxString("12"), runOnDemoGraph("return toString(12) as value").records().next()("value"))
  }
  @Test
  def testSubString(): Unit ={
    Assert.assertEquals(LynxString("llo"), runOnDemoGraph("return substring('hello', 2) as value").records().next()("value"))
    Assert.assertEquals(LynxString("el"), runOnDemoGraph("return substring('hello', 1, 2) as value").records().next()("value"))
  }
  @Test
  def testTo(): Unit ={
    Assert.assertEquals(LynxInteger(10), runOnDemoGraph("return toInteger('10.2') as value").records().next()("value"))
    Assert.assertEquals(LynxFloat(10.2), runOnDemoGraph("return toFloat('10.2') as value").records().next()("value"))
    Assert.assertEquals(LynxBoolean(false), runOnDemoGraph("return toBoolean('false') as value").records().next()("value"))
  }

  // TODO: bug
  @Test
  def testNodes(): Unit ={
    val result = runOnDemoGraph("match p = (a)-->(b)-->(c)-->(d) return nodes(p) as nodes, a, b, c, d;").records().next()
    val actualNodeList = result("nodes").asInstanceOf[LynxList].value
    val expectedNodeList = List( result("a"), result("b"), result("c"), result("d"))
    Assert.assertEquals(expectedNodeList, actualNodeList)
  }

  @Test
  def testRelationships(): Unit = {
    val result1 = runOnDemoGraph("match p = (a{name:'bluejoe'})-[r:KNOWS]->(b) return relationships(p) as rels, r;").records().next()
    Assert.assertEquals(1, result1("rels").asInstanceOf[LynxList].value.length)
    Assert.assertEquals(result1("r"), result1("rels").asInstanceOf[LynxList].value.head)

    val result2 = runOnDemoGraph("match p = (a{name:'bluejoe'})-[r1]->(b)-[r2]->(c)-[r3]->(d) return relationships(p) as rels, r1, r2, r3;").records().next()
    Assert.assertEquals(List(result2("r1"), result2("r2"), result2("r3")), result2("rels").asInstanceOf[LynxList].value)
  }

  @Test
  def testLength(): Unit = {
    Assert.assertEquals(LynxInteger(2), runOnDemoGraph("Match p = ()-->()-->() return length(p) as length;").records().next()("length"))
//    Assert.assertEquals(LynxInteger(1), runOnDemoGraph("Match p = ()-[:KNOWS]->() return length(p) as length;").records().next()("length"))
    Assert.assertEquals(false, runOnDemoGraph("Match p = ()-[:NOT_KNOW]-() return length(p);").records().hasNext)
  }
}
