package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship}
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer

class Parameters extends TestBase{

  val nodeInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Johan")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Michael")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("michael")))

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()
    nodeInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodeInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodeInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))

    model.write.createElements(nodeInput, relationshipInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      })
  }


  @Test
  def stringLiteral_1(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name = $name
        |RETURN n
        |""".stripMargin,Map(("name"->"Johan"))).show()
  }

  @Test
  def stringLiteral_2(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person { name: $name })
        |RETURN n
        |""".stripMargin, Map(("name" -> "Johan"))).show()
  }

  @Test
  def regularExpression():Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name =~ $regex
        |RETURN n.name
        |""".stripMargin, Map(("regex" -> ".*h.*"))).show()
  }

  @Test
  def caseSensitiveStringPatternMatching(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name STARTS WITH $name
        |RETURN n.name
        |""".stripMargin, Map(("name" -> "Michael"))).show()
  }

  @Test
  def createNodeWithProperties(): Unit = {
    runOnDemoGraph(
      """
        |CREATE ($props)
        |""".stripMargin, Map("props" -> List(Map("name" -> "Andy"), Map("position" -> "Developer")))).show()
  }

  @Test
  def createMultipleNodesWithProperties(): Unit = {
    runOnDemoGraph(
      """
        |UNWIND $props AS properties
        |CREATE (n:Person)
        |SET n = properties
        |RETURN n
        |""".stripMargin, Map("props" -> List(Map("awesome" -> "true","name" -> "Andy","position" -> "Developer"),
        Map("children" -> "3","name" -> "Michael","position" -> "Developer")))).show()
  }

  @Test
  def settingAllPropertiesOnANode(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name='Michaela'
        |SET n = $props
        |""".stripMargin, Map("props" -> List(Map("name" -> "Andy"), Map("position" -> "Developer")))).show()
  }

  @Test
  def skipAndLimit(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN n.name
        |SKIP $s
        |LIMIT $l
        |""".stripMargin,Map("s" -> 1,"l" -> 1)).show()
  }

  @Test
  def nodeId(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n)= $id
        |RETURN n.name
        |""".stripMargin, Map("id" -> 1)).show()
  }

  @Test
  def multipleNodeIds(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n) IN $ids
        |RETURN n.name
        |""".stripMargin, Map("ids" -> List(1,2,3))).show()
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
    runOnDemoGraph(
      """
        |START n=node:people(name = $value)
        |RETURN n
        |""".stripMargin, Map(("value" -> "Michaela"))).show()
  }

  @Test
  def indexQuery_explicitIndexes(): Unit = {
    runOnDemoGraph(
      """
        |START n=node:people($query)
        |RETURN n
        |""".stripMargin, Map(("query" -> "name:Bob"))).show()
  }

}
