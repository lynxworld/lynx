import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-18 09:23
 */
class TestDelete {
  val n1 = TestNode(1, Array("Person"), ("name",LynxString("Andy")), ("age", LynxInteger(36)))
  val n2 = TestNode(2, Array("Person"), ("name",LynxString("UNKNOWN")))
  val n3 = TestNode(3, Array("Person"), ("name",LynxString("Timothy")), ("age", LynxInteger(25)))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Peter")), ("age", LynxInteger(34)))

  val r1 = TestRelationship(1, 1, 3, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 4, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4)
  relsBuffer.append(r1, r2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def deleteSingleNode(): Unit ={
    val num = testBase.NODE_SIZE
    testBase.runOnDemoGraph(
      """
        |MATCH (n:Person {name: 'UNKNOWN'})
        |DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, nodesBuffer.size)
  }

  @Test
  def deleteAllNodesAndRelationships(): Unit ={
    testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(0, nodesBuffer.size)
    Assert.assertEquals(0, relsBuffer.size)
  }

  @Test
  def deleteANodeWithAllItsRelationships(): Unit ={
    val num = testBase.NODE_SIZE

    testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, nodesBuffer.size)
    Assert.assertEquals(0, relsBuffer.size)
  }

  @Test
  def deleteRelationshipsOnly(): Unit ={
    val num = testBase.NODE_SIZE

    testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})-[r:KNOWS]->()
        |DELETE r
        |""".stripMargin)

    Assert.assertEquals(num, nodesBuffer.size)
    Assert.assertEquals(0, relsBuffer.size)
  }
}
