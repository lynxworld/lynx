package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

class Parameters extends TestBase{

  val nodeInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Johan")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Michael")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("michael")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Bob")))
  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()
    nodeInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodeInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodeInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodeInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))

    model.write.createElements(nodeInput, relationshipInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      })
  }


  @Test
  def stringLiteral_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name = $name
        |RETURN n
        |""".stripMargin,Map(("name"->"Johan"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(n1, records(0))
  }

  @Test
  def stringLiteral_2(): Unit = {
    val records =  runOnDemoGraph(
      """
        |MATCH (n:Person { name: $name })
        |RETURN n
        |""".stripMargin, Map(("name" -> "Johan"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(n1, records(0))
  }

  @Test
  def regularExpression():Unit = {
    val records =  runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name =~ $regex
        |RETURN n.name
        |""".stripMargin, Map(("regex" -> ".*h.*"))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set( "Johan","Michael","michael"), records.toSet)
  }

  @Test
  def caseSensitiveStringPatternMatching(): Unit = {
    val records =  runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name STARTS WITH $name
        |RETURN n.name
        |""".stripMargin, Map(("name" -> "Michael"))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(Set("Michael"), records.toSet)
  }

  @Test
  def createNodeWithProperties(): Unit = {
    val num = nodeInput.length
    val records = runOnDemoGraph(
      """
        |CREATE ($props)
        |""".stripMargin, Map("props" -> List(Map("name" -> "Andy"), Map("position" -> "Developer")))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(num + 1, all_nodes.size)
    Assert.assertEquals("Andy",records(0).props(LynxPropertyKey("name")).toString)
  }

  @Test
  def createMultipleNodesWithProperties(): Unit = {
    val num = nodeInput.length
    val records = runOnDemoGraph(
      """
        |UNWIND $props AS properties
        |CREATE (n:Person)
        |SET n = properties
        |RETURN n
        |""".stripMargin, Map("props" -> List(Map("awesome" -> "true","name" -> "Andy","position" -> "Developer"),
        Map("children" -> "3","name" -> "Michael","position" -> "Developer"))))
      .records().map(f => f("n").asInstanceOf[TestNode]).toArray

    Assert.assertEquals(num + 2, all_nodes.size)
    Assert.assertEquals(2, records.length)
    Assert.assertEquals("Andy",records(0).props(LynxPropertyKey("name")).toString)
    Assert.assertEquals("Michael",records(1).props(LynxPropertyKey("name")).toString)
  }

  @Test
  def settingAllPropertiesOnANode(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name='Michael'
        |SET n = $props
        |""".stripMargin,
      Map("props" -> List(Map("name" -> "Andy"), Map("position" -> "Developer"))))
      .records().map(f => f("n").asInstanceOf[TestNode]).toArray

    Assert.assertEquals("Andy",records(0).props(LynxPropertyKey("name")).toString)
    Assert.assertEquals("Developer",records(0).props(LynxPropertyKey("position")).toString)
  }

  @Test
  def skipAndLimit(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN n.name
        |SKIP $s
        |LIMIT $l
        |""".stripMargin,Map("s" -> 1,"l" -> 1)).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(Set("michael"), records.toSet)
  }

  @Test
  def nodeId(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n)= $id
        |RETURN n.name
        |""".stripMargin, Map("id" -> 1)).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(Set("Johan"), records.toSet)
  }

  @Test
  def multipleNodeIds(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n) IN $ids
        |RETURN n.name
        |""".stripMargin, Map("ids" -> List(1,2,3))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Johan", "Michael", "michael"), records.toSet)
  }

  @Test
  def callingProcedures(): Unit = {
    runOnDemoGraph(
      """
        |CALL db.resampleIndex($indexname)
        |""".stripMargin, Map(("indexname" -> ":Person(name)"))).show()
  }

  @Test
  def indexValue_explicitIndexes(): Unit = {
    val records = runOnDemoGraph(
      """
        |START n=node:people(name = $value)
        |RETURN n
        |""".stripMargin, Map(("value" -> "Michael"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(n2, records(0))
  }

  @Test
  def indexQuery_explicitIndexes(): Unit = {
    val records = runOnDemoGraph(
      """
        |START n=node:people($query)
        |RETURN n
        |""".stripMargin, Map(("query" -> "name:Bob"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(n4, records(0))
  }

}
