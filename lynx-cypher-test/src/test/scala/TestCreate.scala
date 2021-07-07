import org.grapheco.lynx.{LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-06 17:35
 */
class TestCreate {
  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()

  val n1 = TestNode(1, Seq("Person"), ("name",LynxString("A")))
  val n2 = TestNode(2, Seq("Person"), ("name",LynxString("B")))

  nodesBuffer.append(n1, n2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def createSingleNode(): Unit ={
    val num = nodesBuffer.length
    testBase.runOnDemoGraph(
      """
        |create (n)
        |""".stripMargin)

    Assert.assertEquals(num + 1, nodesBuffer.length)
  }

  @Test
  def createMultipleNodes(): Unit ={
    val num = nodesBuffer.length
    testBase.runOnDemoGraph(
      """
        |CREATE (n), (m)
        |""".stripMargin)

    Assert.assertEquals(num + 2, nodesBuffer.length)
  }

  @Test
  def CreateANodeWithALabel(): Unit ={
    val num = nodesBuffer.length
    testBase.runOnDemoGraph(
      """
        |CREATE (n:Person)
        |""".stripMargin)
    Assert.assertEquals(num + 1, nodesBuffer.length)
  }

  @Test
  def CreateNodeWithMultipleLabel(): Unit ={
    val num = nodesBuffer.length
    testBase.runOnDemoGraph(
      """
        |CREATE (n:Person:Swedish)
        |""".stripMargin)
    Assert.assertEquals(num + 1, nodesBuffer.length)
  }

  @Test
  def CreateNodeAndAddLabelsAndProperty(): Unit ={
    val num = nodesBuffer.length
    testBase.runOnDemoGraph(
      """
        |CREATE (n:Person {name: 'Andy', title: 'Developer'})
        |""".stripMargin)
    Assert.assertEquals(num + 1, nodesBuffer.length)
  }

  @Test
  def returnCreatedNode(): Unit ={
    val num = nodesBuffer.length

    val res = testBase.runOnDemoGraph(
      """
        |CREATE (a {name: 'Andy'})
        |RETURN a.name
        |""".stripMargin).records().toArray
    Assert.assertEquals(num + 1, nodesBuffer.length)
    Assert.assertEquals("Andy", res(0)("a.name").asInstanceOf[LynxValue].value)
  }

  @Test
  def createRelationshipBetweenTwoNodes(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (a:Person),
        |  (b:Person)
        |WHERE a.name = 'A' AND b.name = 'B'
        |CREATE (a)-[r:RELTYPE]->(b)
        |RETURN type(r)
        |""".stripMargin).records().toArray

    Assert.assertEquals("RELTYPE", res(0)("type(r)").asInstanceOf[LynxValue].value)
  }

  @Test
  def createRelationshipAndSetProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (a:Person),
        |  (b:Person)
        |WHERE a.name = 'A' AND b.name = 'B'
        |CREATE (a)-[r:RELTYPE {name: a.name + '<->' + b.name}]->(b)
        |RETURN type(r), r.name
        |""".stripMargin).records().toArray

    Assert.assertEquals("RELTYPE", res(0)("type(r)").asInstanceOf[LynxValue].value)
    Assert.assertEquals("A<->B", res(0)("r.name").asInstanceOf[LynxValue].value)
  }

  @Test
  def createFullPath(): Unit ={
    val numNode = nodesBuffer.length
    val numRels = relsBuffer.length
    val res = testBase.runOnDemoGraph(
      """
        |CREATE p = (andy {name:'Andy'})-[:WORKS_AT]->(neo)<-[:WORKS_AT]-(michael {name: 'Michael'})
        |RETURN p
        |""".stripMargin).records().toArray
    Assert.assertEquals(numNode + 3, nodesBuffer.length)
    Assert.assertEquals(numRels + 2, relsBuffer.length)
  }
}
