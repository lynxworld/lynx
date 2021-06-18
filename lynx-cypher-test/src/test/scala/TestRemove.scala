import org.grapheco.lynx.{LynxInteger, LynxNull, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-18 09:23
 */
class TestRemove {
  val n1 = TestNode(1, Array("Swedish"), ("name",LynxString("Andy")), ("age", LynxInteger(36)))
  val n2 = TestNode(2, Array("Swedish", "German"), ("name",LynxString("Peter")), ("age", LynxInteger(34)))
  val n3 = TestNode(3, Array("Swedish"), ("name",LynxString("Timothy")), ("age", LynxInteger(25)))

  val r1 = TestRelationship(1, 1, 2, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3)
  relsBuffer.append(r1, r2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def removeAProperty(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (a {name: 'Andy'})
        |REMOVE a.age
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("a.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(LynxNull, records.head("a.age").asInstanceOf[LynxValue])
  }

  @Test
  def removeALabelFromANode(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Peter'})
        |REMOVE n:German
        |RETURN n.name, labels(n)
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List("Swedish"), records.head("labels(n)").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }

  @Test
  def removeMultipleLabelsFromANode(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Peter'})
        |REMOVE n:German:Swedish
        |RETURN n.name, labels(n)
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List.empty, records.head("labels(n)").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }
}
