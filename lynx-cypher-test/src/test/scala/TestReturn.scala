import org.grapheco.lynx.{LynxInteger, LynxList, LynxNode, LynxRelationship, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-05 08:49
 */
class TestReturn {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("A")), ("age", LynxInteger(55)), ("happy", LynxString("YES!")))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("B")))

  val r1 = TestRelationship(1, 1, 2, Option("BLOCKS"))
  val r2 = TestRelationship(2, 1, 2, Option("KNOWS"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2)
  relsBuffer.append(r1, r2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def returnNodes(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'B'})
        |RETURN n
        |""".stripMargin).records().toArray

    Assert.assertEquals(n2, res(0)("n").asInstanceOf[LynxNode])
  }

  @Test
  def returnRelationships(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'A'})-[r:KNOWS]->(c)
        |RETURN r
        |""".stripMargin).records().toArray

    Assert.assertEquals(r2, res(0)("r").asInstanceOf[LynxRelationship])
  }

  @Test
  def returnProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'A'})
        |RETURN n.name
        |""".stripMargin).records().toArray

    Assert.assertEquals(n1.property("name").get, res(0)("n.name").asInstanceOf[LynxValue])
  }

  @Test
  def returnAllElement(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH p = (a {name: 'A'})-[r]->(b)
        |RETURN *
        |""".stripMargin).records().toArray

    Assert.assertEquals(n1, res(0)("a").asInstanceOf[LynxNode])
    Assert.assertEquals(n2, res(0)("b").asInstanceOf[LynxNode])
    Assert.assertEquals(r1, res(0)("r").asInstanceOf[LynxRelationship])
    Assert.assertEquals(r2, res(1)("r").asInstanceOf[LynxRelationship])
    Assert.assertEquals(List(n1, LynxList(List(r1, n2, LynxList(List())))), res(0)("p").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List(n1, LynxList(List(r2, n2, LynxList(List())))), res(1)("p").asInstanceOf[LynxValue].value)
  }

  @Test
  def variableWithUncommonCharacters(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (`This isn\'t a common variable`)
        |WHERE `This isn\'t a common variable`.name = 'A'
        |RETURN `This isn\'t a common variable`.happy
        |""".stripMargin).records().toArray

    Assert.assertEquals("YES!", res(0)("`This isn\\'t a common variable`.happy").asInstanceOf[LynxValue].value)
  }

  @Test
  def columnAlias(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})
        |RETURN a.age AS SomethingTotallyDifferent
        |""".stripMargin).records().toArray

    Assert.assertEquals(55L, res(0)("SomethingTotallyDifferent").asInstanceOf[LynxValue].value)
  }

  @Test
  def optionalProperties(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(55L, res(0)("n.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(1)("n.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def uniqueResults(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})-->(b)
        |RETURN DISTINCT b
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, res.length)
    Assert.assertEquals(n2, res(0)("b").asInstanceOf[LynxNode])
  }

  @Test
  def otherExpressions(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})
        |RETURN a.age > 30, "I'm a literal", (a)-->()
        |""".stripMargin).records().toArray
  }
}
