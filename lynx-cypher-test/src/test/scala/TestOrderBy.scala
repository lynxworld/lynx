import org.grapheco.lynx.{LynxInteger, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-06 15:24
 */
class TestOrderBy {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("A")), ("age", LynxInteger(34)), ("length", LynxInteger(170)))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("B")), ("age", LynxInteger(36)))
  val n3 = TestNode(3, Seq.empty, ("name",LynxString("C")), ("age", LynxInteger(32)), ("length", LynxInteger(185)))

  val r1 = TestRelationship(1, 1, 2, Option("BLOCKS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3)
  relsBuffer.append(r1, r2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def orderNodesByProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.name
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("A", 34), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("C", 32), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderNodesByMultipleProperties(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.age, n.name
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", 32), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("A", 34), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderNodesInDescendingOrder(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.name DESC
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", 32), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("A", 34), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderingNull(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.length, n.name, n.age
        |ORDER BY n.length
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(170, "A", 34), List(res(0)("n.length"), res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List(185, "C", 32), List(res(1)("n.length"), res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List(null, "B", 36), List(res(2)("n.length"), res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderingInAWITHClause(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |WITH n ORDER BY n.age
        |RETURN collect(n.name) AS names
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", "A", "B").map(LynxValue(_)), res(0)("names").asInstanceOf[LynxValue].value)
  }
}
