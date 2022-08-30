package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

class A_Predicate extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Alice"),
      LynxPropertyKey("age") -> LynxValue("38"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n2 = TestNode(TestId(2), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Charlie"),
      LynxPropertyKey("age") -> LynxValue("53"),
      LynxPropertyKey("eyes") -> LynxValue("green")))
  val n3 = TestNode(TestId(3), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Bob"),
      LynxPropertyKey("age") -> LynxValue("25"),
      LynxPropertyKey("eyes") -> LynxValue("blue")))
  val n4 = TestNode(TestId(4), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Daniel"),
      LynxPropertyKey("age") -> LynxValue("54"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n5 = TestNode(TestId(5), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Eskil"),
      LynxPropertyKey("age") -> LynxValue("38"),
      LynxPropertyKey("eyes") -> LynxValue("brown"),
      LynxPropertyKey("array") -> LynxValue(Array("one", "two", "three"))))
  val n6 = TestNode(TestId(6), Seq.empty,
    Map(LynxPropertyKey("age") -> LynxValue("61"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("MARRIED")), Map.empty)

  @Before
  def init(): Unit = {
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))
    nodesInput.append(("n6", NodeInput(n6.labels, n6.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), Seq.empty, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), Seq.empty, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), Seq.empty, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), Seq.empty, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def all(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH  p =(a)-[*1..3]->(b)
        |WHERE a.name = 'Alice' AND b.name = 'Daniel' AND ALL (x IN nodes(p) WHERE x.age > 30)
        |RETURN p
        |""".stripMargin).records().map(f => f("p").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    //   should be "(0)-[KNOWS,1]->(2)-[KNOWS,3]->(3)"
  }

  @Test
  def any(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Eskil' AND ANY (x IN a.array WHERE x = 'one')
        |RETURN a.name, a.array
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Eskil", records(0)("a.name").asInstanceOf[LynxValue].value)
    //    Assert.assertEquals("Oliver Stone", records.head("a.array").asInstanceOf[LynxValue].value)
  }

  @Test
  def exists1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH n
        |WHERE exists(n.name)
        |RETURN n.name AS name, exists((n)-[:MARRIED]->()) AS is_married
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    for (record <- records) {
      record("name").asInstanceOf[LynxValue].value match {
        case "Alice" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Bob" => Assert.assertEquals(true, record("is_married").asInstanceOf[LynxValue].value)
        case "Charlie" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Daniel" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Eskil" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case _ => Assert.assertEquals(false, true)
      }
    }
  }

  @Test
  def exists2(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (a),(b)
        |WHERE exists(a.name) AND NOT exists(b.name)
        |OPTIONAL MATCH (c:DoesNotExist)
        |RETURN a.name AS a_name, b.name AS b_name, exists(b.name) AS b_has_name, c.name AS c_name, exists(c.name) AS c_has_name
        |ORDER BY a_name, b_name, c_name
        |LIMIT 1
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Alice", records(0)("is_married").asInstanceOf[LynxValue].value)
      Assert.assertEquals(null, records(0)("b_name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(false, records(0)("b_has_name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records(0)("c_name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, records(0)("c_has_name").asInstanceOf[LynxValue].value)
  }

  @Test
  def none(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH p =(n)-[*1..3]->(b)
        |WHERE n.name = 'Alice' AND NONE (x IN nodes(p) WHERE x.age = 25)
        |RETURN p
        |""".stripMargin).records().map(f => f("p").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(2, records.length)
    Assert.assertEquals(List(n1, LynxList(List(r2, n2, LynxList(List())))), records(0))
    Assert.assertEquals(List(n1, LynxList(List(r2, n2, LynxList(List(r3, n2, LynxList(List())))))), records(1))
  }

  @Test
  def signle(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH p =(n)-->(b)
        |WHERE n.name = 'Alice' AND SINGLE (var IN nodes(p) WHERE var.eyes = 'blue')
        |RETURN p
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(n1, LynxList(List(r1, n2, LynxList(List())))), records(0))
  }
}

