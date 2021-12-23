import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-06 16:24
 */
class TestSkip {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("A")))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("B")))
  val n3 = TestNode(3, Seq.empty, ("name",LynxString("C")))
  val n4 = TestNode(4, Seq.empty, ("name",LynxString("D")))
  val n5 = TestNode(5, Seq.empty, ("name",LynxString("E")))

  val r1 = TestRelationship(1, 1, 2, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 1, 4, Option("KNOWS"))
  val r4 = TestRelationship(4, 1, 5, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5)
  relsBuffer.append(r1, r2, r3, r4)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def skipFirstThreeRows(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name
        |ORDER BY n.name
        |SKIP 3
        |""".stripMargin).records().toArray

    Assert.assertEquals(LynxString("D"), res(0)("n.name"))
    Assert.assertEquals(LynxString("E"), res(1)("n.name"))
  }

  @Test
  def returnMiddleTwoRows(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name
        |ORDER BY n.name
        |SKIP 1
        |LIMIT 2
        |""".stripMargin).records().toArray

    Assert.assertEquals(LynxString("B"), res(0)("n.name"))
    Assert.assertEquals(LynxString("C"), res(1)("n.name"))
  }

  @Test
  def UsingAnExpressionWithSKIPToReturnASubsetOfTheRows(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name
        |ORDER BY n.name
        |SKIP 1 + toInteger(3*rand())
        |""".stripMargin).records().toArray
  }
}
