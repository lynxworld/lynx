package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:16
 */
class L_Set extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("Stefan")))
  val n2 = TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("George")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Swedish")), Map(LynxPropertyKey("name")-> LynxValue("Andy"), LynxPropertyKey("age")-> LynxValue(36), LynxPropertyKey("hungry")->LynxValue(true)))
  val n4 = TestNode(TestId(4), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("Peter"), LynxPropertyKey("age")-> LynxValue(34)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(2), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)

  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def setAProperty1(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.surname = 'Taylor'
        |RETURN n.name, n.surname
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Taylor", records.head("n.surname").asInstanceOf[LynxValue].value)
  }

  @Test
  def setAProperty2(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET (CASE WHEN n.age = 36 THEN n END).worksIn = 'Malmo'
        |RETURN n.name, n.worksIn
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Malmo", records.head("n.worksIn").asInstanceOf[LynxValue].value)
  }

  @Test
  def setAProperty3(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET (CASE WHEN n.age = 55 THEN n END).worksIn = 'Malmo'
        |RETURN n.name, n.worksIn
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records.head("n.worksIn").asInstanceOf[LynxValue].value)
  }

  @Test
  def updateAProperty(): Unit ={
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |SET n.position = 'Developer', n.surname = 'Taylor'
        |""".stripMargin).records().toArray

//    Assert.assertEquals("Developer", all_nodes(2).property(LynxPropertyKey("position")).get.value)
//    Assert.assertEquals("Taylor", all_nodes(2).property(LynxPropertyKey("surname")).get.value)
  }

  @Test
  def setALabelOnANode(): Unit ={
    val records = runOnDemoGraph(
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
    val records = runOnDemoGraph(
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
