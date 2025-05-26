package org.grapheco.lynx

import org.grapheco.lynx.procedure.UnknownProcedureException
import org.grapheco.lynx.types.time.LynxDuration
import org.junit.jupiter.api.function.Executable
//import org.grapheco.lynx.util.LynxDurationUtil
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}

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
    Assertions.assertEquals(Seq("name"), rs.columns)
    Assertions.assertEquals(1, rs.records().size)
    Assertions.assertEquals(Map("name" ->LynxList(List(LynxString("bluejoe"), LynxString("lzx"), LynxString("airzihao")))), rs.records().toSeq.apply(0))
  }

  @Test
  def testFullTextIndexQuery(): Unit = {
    var rs = runOnDemoGraph("call db.index.fulltext.queryNodes(\"111\", \"222\")")
    Assertions.assertEquals(1, rs.records().size)
  }

  @Test
  def testFullTextIndexCreate(): Unit = {
    var rs = runOnDemoGraph("call db.index.fulltext.createNodeIndex(\"111\", [\"222\"],  [\"333\"])")
    Assertions.assertEquals(1, rs.records().size)


  }

  @Test
  def testWrongCall(): Unit = {
    Assertions.assertThrows(classOf[UnknownProcedureException], () => runOnDemoGraph("call test.nonexisting()"))

    Assertions.assertThrows(classOf[UnknownProcedureException], ()=> runOnDemoGraph("call test.authors(2)"))
  }

  @Test
  def testCountSimple(): Unit = {
    var rs = runOnDemoGraph("match (n) return count(n)").records().next()("count(n)")
    Assertions.assertEquals(LynxInteger(4), rs)
  }

  @Test
  def testSumSimple(): Unit = {
    val rs = runOnDemoGraph("match (n) return sum(n.age)").records().next()("sum(n.age)")
    Assertions.assertEquals(LynxFloat(90), rs)
  }

  @Test
  def testSumEmpty(): Unit = {
    val rs = runOnDemoGraph("match (n:notexists) return sum(n.age)").records().next()("sum(n.age)")
    Assertions.assertEquals(LynxFloat(0), rs)
  }

  @Test
  def testAvg(): Unit = {
    val rs = runOnDemoGraph("match (n) return avg(n.age)").records().next()("avg(n.age)")
    Assertions.assertEquals(LynxFloat(90/4.0), rs)
  }

  @Test
  def testNoneAvg(): Unit = {
    val rs = runOnDemoGraph("match (n:notexists) return avg(n.age)").records().next()("avg(n.age)")
    Assertions.assertEquals(LynxNull, rs)
  }

  @Test
  def testDuration(): Unit = {
    runOnDemoGraph("CREATE (:profile {works: duration('P18DT16H12M'), history: duration({years: 10.2, months: 5, days: 14, hours:16, minutes: 12})})")
    runOnDemoGraph("CREATE (:profile {works: duration('P10DT16H12M'), history: duration({seconds: 1, milliseconds: 123, microseconds: 456, nanoseconds: 789})})")
    val rs = runOnDemoGraph("match (n:profile) return avg(n.works), sum(n.history)").records().next()
    Assertions.assertEquals(LynxDuration.parse("P14DT16H12M"), rs("avg(n.works)"))
    Assertions.assertEquals(LynxDuration.parse("PT93304H12M1.123123725S"), rs("sum(n.history)"))
  }

  @Test
  def testScalarSize(): Unit = {
    val rs = runOnDemoGraph("RETURN size(['Alice', 'Bob'])").records().next().get(0).get
    Assertions.assertEquals(LynxInteger(2), rs)
  }

  @Test
  def testStringSize(): Unit = {
    val rs = runOnDemoGraph("RETURN size('Alice')").records().next().get(0).get
    Assertions.assertEquals(LynxInteger(5), rs)
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
    Assertions.assertEquals(LynxInteger(10), runOnDemoGraph(s"return abs(${20 - 30}) as value").records().next()("value"))
    Assertions.assertEquals(LynxFloat(9.8), runOnDemoGraph(s"return abs(${20.2 - 30}) as value").records().next()("value"))
  }

  @Test
  def testCeil(): Unit ={
    Assertions.assertEquals(LynxFloat(1.0), runOnDemoGraph(s"return ceil(0.1) as value").records().next()("value"))
  }

  @Test
  def testFloor(): Unit ={
    Assertions.assertEquals(LynxFloat(0.0), runOnDemoGraph(s"return floor(0.9) as value").records().next()("value"))
  }

  @Test
  def testRand(): Unit ={
    runOnDemoGraph(s"return rand() as value").show()
  }

  @Test
  def testRound(): Unit ={
    Assertions.assertEquals(LynxInteger(3), runOnDemoGraph(s"return round(3.141592) as value").records().next()("value"))
  }
  @Test
  def testRoundWithPrecision(): Unit ={
    Assertions.assertEquals(LynxFloat(3.142), runOnDemoGraph(s"return round(3.141592, 3) as value").records().next()("value"))
  }

  @Test
  def testSign(): Unit ={
    Assertions.assertEquals(LynxFloat(-1.0), runOnDemoGraph(s"return sign(-17) as value").records().next()("value"))
    Assertions.assertEquals(LynxFloat(1.0), runOnDemoGraph(s"return sign(17) as value").records().next()("value"))
    Assertions.assertEquals(LynxFloat(0.0), runOnDemoGraph(s"return sign(0) as value").records().next()("value"))
  }

  @Test
  def testE(): Unit ={
    Assertions.assertEquals(LynxFloat(Math.E), runOnDemoGraph(s"return e() as value").records().next()("value"))
  }

  @Test
  def testExp(): Unit ={
    Assertions.assertEquals(LynxFloat(7.38905609893065), runOnDemoGraph(s"return exp(2) as value").records().next()("value"))
  }

  @Test
  def testLog(): Unit ={
    Assertions.assertEquals(LynxFloat(3.295836866004329), runOnDemoGraph(s"return log(27) as value").records().next()("value"))
  }

  @Test
  def testLog10(): Unit ={
    Assertions.assertEquals(LynxFloat(1.4313637641589874), runOnDemoGraph(s"return log10(27) as value").records().next()("value"))
  }

  @Test
  def testLogb(): Unit ={
    Assertions.assertEquals(LynxFloat(3.0), runOnDemoGraph(s"return logb(8,2) as value").records().next()("value"))
  }

  @Test
  def testSqrt(): Unit ={
    Assertions.assertEquals(LynxFloat(16.0), runOnDemoGraph(s"return sqrt(256) as value").records().next()("value"))
  }

  @Test
  def testAcos(): Unit ={
    Assertions.assertEquals(LynxFloat(1.0471975511965979), runOnDemoGraph(s"return acos(0.5) as value").records().next()("value"))
  }

  @Test
  def testAsin(): Unit ={
    Assertions.assertEquals(LynxFloat(0.5235987755982989), runOnDemoGraph(s"return asin(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan(): Unit ={
    Assertions.assertEquals(LynxFloat(0.4636476090008061), runOnDemoGraph(s"return atan(0.5) as value").records().next()("value"))
  }

  @Test
  def testAtan2(): Unit ={
    Assertions.assertEquals(LynxFloat(0.6947382761967033), runOnDemoGraph(s"return atan2(0.5, 0.6) as value").records().next()("value"))
  }

  @Test
  def testCos(): Unit ={
    Assertions.assertEquals(LynxFloat(0.8775825618903728), runOnDemoGraph(s"return cos(0.5) as value").records().next()("value"))
  }

  @Test
  def testCot(): Unit ={
    Assertions.assertEquals(LynxFloat(1.830487721712452), runOnDemoGraph(s"return cot(0.5) as value").records().next()("value"))
  }

  @Test
  def testHaversin(): Unit ={
    Assertions.assertEquals(LynxFloat(0.06120871905481362), runOnDemoGraph("return haversin(0.5) as value").records().next()("value"))
  }

  @Test
  def testPi(): Unit ={
    Assertions.assertEquals(LynxFloat(Math.PI), runOnDemoGraph(s"return pi() as value").records().next()("value"))
  }

  @Test
  def testRadians(): Unit ={
    Assertions.assertEquals(LynxFloat(3.141592653589793), runOnDemoGraph(s"return radians(180) as value").records().next()("value"))
  }

  @Test
  def testSin(): Unit ={
    Assertions.assertEquals(LynxFloat(0.479425538604203), runOnDemoGraph(s"return sin(0.5) as value").records().next()("value"))
  }

  @Test
  def testTan(): Unit ={
    Assertions.assertEquals(LynxFloat(0.5463024898437905), runOnDemoGraph(s"return tan(0.5) as value").records().next()("value"))
  }

  @Test
  def testSinh(): Unit ={
    Assertions.assertEquals(LynxFloat(0.5210953054937474), runOnDemoGraph(s"return sinh(0.5) as value").records().next()("value"))
  }

  @Test
  def testCosh(): Unit ={
    Assertions.assertEquals(LynxFloat(1.1276259652063807), runOnDemoGraph(s"return cosh(0.5) as value").records().next()("value"))
  }

  @Test
  def testTanh(): Unit ={
    Assertions.assertEquals(LynxFloat(0.46211715726000974), runOnDemoGraph(s"return tanh(0.5) as value").records().next()("value"))
  }
  @Test
  def testComplexQueryWithAggregation(): Unit = {
    runOnDemoGraph("CREATE (a:person {name: 'John', age: 25}), (b:person {name: 'Alice', age: 30}), (c:person {name: 'Bob', age: 35}), (d:person {name: 'Charlie', age: 40})")
    runOnDemoGraph("MATCH (a:person), (b:person) WHERE a.name <> b.name CREATE (a)-[:KNOWS]->(b)")

    val rs = runOnDemoGraph("MATCH (n:person)-[:KNOWS]->() RETURN avg(n.age) AS avg_age, max(n.age) AS max_age, min(n.age) AS min_age").records().next()
    Assertions.assertEquals(LynxFloat(32.5), rs("avg_age"))
    Assertions.assertEquals(LynxInteger(40), rs("max_age"))
    Assertions.assertEquals(LynxInteger(25), rs("min_age"))
  }

  // String Functions
  @Test
  def testLeft(): Unit ={
    Assertions.assertEquals(LynxString("hel"), runOnDemoGraph("return left('hello', 3) as value").records().next()("value"))
  }

  @Test
  def testRight(): Unit ={
    Assertions.assertEquals(LynxString("llo"), runOnDemoGraph("return right('hello', 3) as value").records().next()("value"))
  }

  @Test
  def testTrim(): Unit ={
    Assertions.assertEquals(LynxString("hello"), runOnDemoGraph("return ltrim('    hello') as value").records().next()("value"))
    Assertions.assertEquals(LynxString("hello"), runOnDemoGraph("return rtrim('hello    ') as value").records().next()("value"))
    Assertions.assertEquals(LynxString("hello"), runOnDemoGraph("return trim('    hello  ') as value").records().next()("value"))
  }

  @Test
  def testReplace(): Unit ={
    Assertions.assertEquals(LynxString("hezzo"), runOnDemoGraph("return replace('hello', 'l', 'z') as value").records().next()("value"))
  }

  @Test
  def testReverse(): Unit ={
    Assertions.assertEquals(LynxString("olleh"), runOnDemoGraph("return reverse('hello') as value").records().next()("value"))
  }

  @Test
  def testSplit(): Unit ={
    Assertions.assertEquals(LynxList(List(LynxString("one"), LynxString("two"))), runOnDemoGraph("return split('one,two', ',') as value").records().next()("value"))
  }

  @Test
  def testLowerAndUpper(): Unit ={
    Assertions.assertEquals(LynxString("hello"), runOnDemoGraph("return toLower('HELLO') as value").records().next()("value"))
    Assertions.assertEquals(LynxString("HELLO"), runOnDemoGraph("return toUpper('hello') as value").records().next()("value"))
  }

  @Test
  def testToString(): Unit ={
    Assertions.assertEquals(LynxString("12"), runOnDemoGraph("return toString(12) as value").records().next()("value"))
  }
  @Test
  def testSubString(): Unit ={
    Assertions.assertEquals(LynxString("llo"), runOnDemoGraph("return substring('hello', 2) as value").records().next()("value"))
    Assertions.assertEquals(LynxString("el"), runOnDemoGraph("return substring('hello', 1, 2) as value").records().next()("value"))
  }
  @Test
  def testTo(): Unit ={
    Assertions.assertEquals(LynxInteger(10), runOnDemoGraph("return toInteger('10.2') as value").records().next()("value"))
    Assertions.assertEquals(LynxFloat(10.2), runOnDemoGraph("return toFloat('10.2') as value").records().next()("value"))
    Assertions.assertEquals(LynxBoolean(false), runOnDemoGraph("return toBoolean('false') as value").records().next()("value"))
  }

  // TODO: bug
  @Test
  def testNodes(): Unit ={
    val result = runOnDemoGraph("match p = (a)-->(b)-->(c)-->(d) return nodes(p) as nodes, a, b, c, d;").records().next()
    val actualNodeList = result("nodes").asInstanceOf[LynxList].value
    val expectedNodeList = List( result("a"), result("b"), result("c"), result("d"))
    Assertions.assertEquals(expectedNodeList, actualNodeList)
  }

  @Test
  def testRelationships(): Unit = {
    val result1 = runOnDemoGraph("match p = (a{name:'bluejoe'})-[r:KNOWS]->(b) return relationships(p) as rels, r;").records().next()
    Assertions.assertEquals(1, result1("rels").asInstanceOf[LynxList].value.length)
    Assertions.assertEquals(result1("r"), result1("rels").asInstanceOf[LynxList].value.head)

    val result2 = runOnDemoGraph("match p = (a{name:'bluejoe'})-[r1]->(b)-[r2]->(c)-[r3]->(d) return relationships(p) as rels, r1, r2, r3;").records().next()
    Assertions.assertEquals(List(result2("r1"), result2("r2"), result2("r3")), result2("rels").asInstanceOf[LynxList].value)
  }

  @Test
  def testLength(): Unit = {
    Assertions.assertEquals(LynxInteger(2), runOnDemoGraph("Match p = ()-->()-->() return length(p) as length;").records().next()("length"))
//    Assertions.assertEquals(LynxInteger(1), runOnDemoGraph("Match p = ()-[:KNOWS]->() return length(p) as length;").records().next()("length"))
    Assertions.assertEquals(false, runOnDemoGraph("Match p = ()-[:NOT_KNOW]-() return length(p);").records().hasNext)
  }
}


