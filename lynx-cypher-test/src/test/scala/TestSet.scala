import org.grapheco.lynx.{LynxBoolean, LynxInteger, LynxNull, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-18 09:22
 */
class TestSet {

  val n1 = TestNode(1, Seq.empty, ("name", LynxString("Stefan")))
  val n2 = TestNode(2, Seq.empty, ("name", LynxString("George")))
  val n3 = TestNode(3, Array("Swedish"), ("name",LynxString("Andy")), ("age", LynxInteger(36)), ("hungry", LynxBoolean(true)))
  val n4 = TestNode(4, Seq.empty, ("name",LynxString("Peter")), ("age", LynxInteger(34)))

  val r1 = TestRelationship(1, 1, 3, Option("KNOWS"))
  val r2 = TestRelationship(2, 2, 4, Option("KNOWS"))
  val r3 = TestRelationship(3, 3, 4, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4)
  relsBuffer.append(r1, r2, r3)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def setAProperty(): Unit ={
    var records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.surname = 'Taylor'
        |RETURN n.name, n.surname
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Taylor", records.head("n.surname").asInstanceOf[LynxValue].value)

    records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET (CASE WHEN n.age = 36 THEN n END).worksIn = 'Malmo'
        |RETURN n.name, n.worksIn
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Malmo", records.head("n.worksIn").asInstanceOf[LynxValue].value)

    records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET (CASE WHEN n.age = 55 THEN n END).worksIn = 'Malmo'
        |RETURN n.name, n.worksIn
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(LynxNull, records.head("n.worksIn").asInstanceOf[LynxValue].value)
  }

  @Test
  def updateAProperty(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.age = toString(n.age)
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("36", records.head("n.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def removeAProperty(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.name = null
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(null, records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(36L, records.head("n.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def copyPropertiesBetweenNodesAndRelationships(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (at {name: 'Andy'}),
        |  (pn {name: 'Peter'})
        |SET at = pn
        |RETURN at.name, at.age, at.hungry, pn.name, pn.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("at.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(34L, records.head("at.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records.head("at.hungry").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Peter", records.head("pn.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(34L, records.head("pn.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def replaceAllPropertiesUsingAMap(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (p {name: 'Peter'})
        |SET p = {name: 'Peter Smith', position: 'Entrepreneur'}
        |RETURN p.name, p.age, p.position
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter Smith", records.head("p.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records.head("p.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Entrepreneur", records.head("p.position").asInstanceOf[LynxValue].value)
  }

  @Test
  def removeAllPropertiesUsingAnEmptyMap(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (p {name: 'Peter'})
        |SET p = {}
        |RETURN p.name, p.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(null, records.head("p.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records.head("p.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def mutateSpecificPropertiesUsingAMap1(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (p {name: 'Peter'})
        |SET p += {age: 38, hungry: true, position: 'Entrepreneur'}
        |RETURN p.name, p.age, p.hungry, p.position
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("p.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(38L, records.head("p.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(true, records.head("p.hungry").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Entrepreneur", records.head("p.position").asInstanceOf[LynxValue].value)
  }

  @Test
  def mutateSpecificPropertiesUsingAMap2(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (p {name: 'Peter'})
        |SET p += {}
        |RETURN p.name, p.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("p.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(34L, records.head("p.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def setMultiplePropertiesUsingOneSetClause(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.position = 'Developer', n.surname = 'Taylor'
        |""".stripMargin).records().toArray

    Assert.assertEquals("Developer", nodesBuffer(2).property("position").get.value)
    Assert.assertEquals("Taylor", nodesBuffer(2).property("surname").get.value)
  }

  @Test
  def setALabelOnANode(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Stefan'})
        |SET n:German
        |RETURN n.name, labels(n) AS labels
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Stefan", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List("German"), records.head("labels").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }

  @Test
  def setMultipleLabelsOnANode(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'George'})
        |SET n:Swedish:Bossman
        |RETURN n.name, labels(n) AS labels
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("George", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List("Swedish", "Bossman"), records.head("labels").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }
}
