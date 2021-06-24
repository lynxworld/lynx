import org.grapheco.lynx.{LynxInteger, LynxList, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-24 09:58
 */
class TestCase {
  val n1 = TestNode(1, Array("A"), ("name",LynxString("Alice")), ("age", LynxInteger(38)), ("eyes", LynxString("brown")))
  val n2 = TestNode(2, Array("B"), ("name",LynxString("Bob")), ("age", LynxInteger(25)), ("eyes", LynxString("blue")))
  val n3 = TestNode(3, Array("C"), ("name",LynxString("Charlie")), ("age", LynxInteger(53)), ("eyes", LynxString("green")))
  val n4 = TestNode(4, Array("D"), ("name",LynxString("Daniel")), ("eyes", LynxString("brown")))
  val n5 = TestNode(5, Array("E"), ("name",LynxString("Eskil")), ("array", LynxList(List(LynxString("one"),LynxString("two"),LynxString("three")))), ("age", LynxInteger(41)), ("eyes", LynxString("blue")))

  val r1 = TestRelationship(1, 1, 2, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 2, 4, Option("KNOWS"))
  val r4 = TestRelationship(4, 2, 5, Option("MARRIED"))
  val r5 = TestRelationship(5, 3, 4, Option("KNOWS"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5)
  relsBuffer.append(r1, r2, r3, r4, r5)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def simpleCaseForm(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN
        |CASE n.eyes
        |  WHEN 'blue'  THEN 1
        |  WHEN 'brown' THEN 2
        |  ELSE 3
        |END AS result
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(2,1,3,2,1), records.map(f => f("result").asInstanceOf[LynxValue].value).toList)
  }

  @Test
  def allowingForMultipleConditionalsToBeExpressed(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN
        |CASE
        |  WHEN n.eyes = 'blue' THEN 1
        |  WHEN n.age < 40      THEN 2
        |  ELSE 3
        |END AS result
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(2,1,3,3,1), records.map(f => f("result").asInstanceOf[LynxValue].value).toList)
  }

  @Test
  def distinguishingBetweenWhenToUseTheSimpleAndGenericCASEForms1(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name,
        |CASE n.age
        |  WHEN n.age IS NULL THEN -1
        |  ELSE n.age - 10
        |END AS age_10_years_ago
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(28, 15, 43, null, 31), records.map(f => f("age_10_years_ago").asInstanceOf[LynxValue].value).toList)
  }

  @Test
  def distinguishingBetweenWhenToUseTheSimpleAndGenericCASEForms2(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name,
        |CASE
        |  WHEN n.age IS NULL THEN -1
        |  ELSE n.age - 10
        |END AS age_10_years_ago
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(28, 15, 43, -1, 31), records.map(f => f("age_10_years_ago").asInstanceOf[LynxValue].value).toList)
  }
}
