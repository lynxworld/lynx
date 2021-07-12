import org.grapheco.lynx.{LynxNode, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-12 09:07
 */
class TestOptionMatch {
  val n3 = TestNode(1, Array("Person"), ("name",LynxString("Charlie Sheen")))
  val n1 = TestNode(2, Array("Person"), ("name",LynxString("Oliver Stone")))
  val n2 = TestNode(3, Array("Person"), ("name",LynxString("Michael Douglas")))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Martin Sheen")))
  val n5 = TestNode(5, Array("Person"), ("name",LynxString("Rob Reiner")))
  val m1 = TestNode(6, Array("Movie"), ("title",LynxString("Wall Street")))
  val m2 = TestNode(7, Array("Movie"), ("title",LynxString("The American President")))

  val r1 = TestRelationship(1, 1, 4, Option("FATHER"))
  val r2 = TestRelationship(2, 1, 6, Option("ACTED_IN"))
  val r3 = TestRelationship(3, 2, 6, Option("DIRECTED"))
  val r4 = TestRelationship(4, 3, 6, Option("ACTED_IN"))
  val r5 = TestRelationship(5, 3, 7, Option("ACTED_IN"))
  val r6 = TestRelationship(6, 4, 6, Option("ACTED_IN"))
  val r7 = TestRelationship(7, 4, 7, Option("ACTED_IN"))
  val r8 = TestRelationship(8, 5, 7, Option("DIRECTED"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, m1, m2)
  relsBuffer.append(r1, r2, r3, r4, r5, r6, r7, r8)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def optionalRelationships(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-->(x)
        |RETURN x
        |""".stripMargin).records().toArray
    Assert.assertEquals(null, res(0)("x").asInstanceOf[LynxValue].value)
  }

  @Test
  def propertiesOnOptionalElements(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-->(x)
        |RETURN x, x.name
        |""".stripMargin).records().toArray
    Assert.assertEquals(null, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(0)("x.name").asInstanceOf[LynxValue].value)
  }

  @Test
  def optionalTypedAndNamedRelationship(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-[r:ACTS_IN]->()
        |RETURN a.title, r
        |""".stripMargin).records().toArray
    Assert.assertEquals("Wall Street", res(0)("a.title").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(0)("r").asInstanceOf[LynxValue].value)
  }


}
