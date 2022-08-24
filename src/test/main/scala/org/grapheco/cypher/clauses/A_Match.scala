package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 14:26
 */
class A_Match extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Oliver Stone")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Michael Douglas")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Charlie Sheen")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Martin Sheen")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Rob Reiner")))
  val m1 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("Wall Street")))
  val m2 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("The American President")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(6), Option(LynxRelationshipType("DIRECTED")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(2), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("Gordon Gekko")))
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("President Andrew Shepherd")))
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("Bud Fox")))
  val r5 = TestRelationship(TestId(5), TestId(4), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("Carl Fox")))
  val r6 = TestRelationship(TestId(6), TestId(4), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("A.J. MacInerney")))
  val r7 = TestRelationship(TestId(7), TestId(5), TestId(7), Option(LynxRelationshipType("DIRECTED")), Map.empty)


  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))
    nodesInput.append(("m1", NodeInput(m1.labels, m1.props.toSeq)))
    nodesInput.append(("m2", NodeInput(m2.labels, m2.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), r4.props.toSeq, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), r5.props.toSeq, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))
    relationsInput.append(("r6", RelationshipInput(Seq(r6.relationType.get), r6.props.toSeq, StoredNodeInputRef(r6.startNodeId), StoredNodeInputRef(r6.endNodeId))))
    relationsInput.append(("r7", RelationshipInput(Seq(r7.relationType.get), Seq.empty, StoredNodeInputRef(r7.startNodeId), StoredNodeInputRef(r7.endNodeId))))


    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def getAllNodes(): Unit ={
    val records = runOnDemoGraph("Match (n) Return n").records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assert.assertEquals(7, records.length)
    Assert.assertEquals(n1, records(0))
    Assert.assertEquals(n2, records(1))
    Assert.assertEquals(n3, records(2))
    Assert.assertEquals(n4, records(3))
    Assert.assertEquals(n5, records(4))
    Assert.assertEquals(m1, records(5))
    Assert.assertEquals(m2, records(6))
  }

  @Test
  def getAllNodesWithLabel(): Unit ={
    val records = runOnDemoGraph("match (movie:Movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals("Wall Street", records(0))
    Assert.assertEquals("The American President", records(1))
  }

  @Test
  def relatedNodes(): Unit ={
    val records = runOnDemoGraph("MATCH (director {name: 'Oliver Stone'})--(movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def matchWithLabels(): Unit ={
    val records = runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})--(movie:Movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def outgoingRelationships(): Unit ={
    val records = runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})-->(movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def directedRelationshipsAndVariable(): Unit ={
    val records = runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})-[r]->(movie) return type(r)").records().map(f => f("type(r)").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("DIRECTED", records.head)
  }

  @Test
  def matchOnRelationshipType(): Unit ={
    val records = runOnDemoGraph("MATCH (wallstreet:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(actor) return actor.name").records().map(f => f("actor.name").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Michael Douglas", "Martin Sheen", "Charlie Sheen"), records.toSet)
  }

  @Test
  def matchOnMultipleRelationshipTypes(): Unit ={
    val records = runOnDemoGraph("MATCH (wallstreet {title: 'Wall Street'})<-[:ACTED_IN|:DIRECTED]-(person) return person.name").records().map(f => f("person.name").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(4, records.length)
    Assert.assertEquals(Set("Michael Douglas", "Martin Sheen", "Charlie Sheen", "Oliver Stone"), records.toSet)
  }

  @Test
  def matchOnRelationshipTypeAndUseAVariable(): Unit ={
    val records = runOnDemoGraph("MATCH (wallstreet {title: 'Wall Street'})<-[r:ACTED_IN]-(actor) return r.role").records().map(f => f("r.role").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Gordon Gekko", "Carl Fox", "Bud Fox"), records.toSet)
  }

  @Test
  def relationshipTypesWithUncommonCharacters(): Unit ={
    runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (rob:Person {name: 'Rob Reiner'})
        |CREATE (rob)-[:`TYPE INCLUDING A SPACE`]->(charlie)
        |""".stripMargin)
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Rob Reiner'})-[r:`TYPE INCLUDING A SPACE`]->()
        |RETURN type(r)
        |""".stripMargin).records().map(f => f("type(r)").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("TYPE INCLUDING A SPACE", records.head)
  }

  @Test
  def multipleRelationships(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN]->(movie)<-[:DIRECTED]-(director)
        |RETURN movie.title, director.name
        |""".stripMargin).records().toArray


    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head("movie.title").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Oliver Stone", records.head("director.name").asInstanceOf[LynxValue].value)
  }

  /**
   * Nodes that are a variable number of relationship->node hops away can be found using the following syntax: -[:TYPE*minHops..maxHops]->.
   * minHops and maxHops are optional and default to 1 and infinity respectively.
   * When no bounds are given the dots may be omitted.
   * The dots may also be omitted when setting only one bound and this implies a fixed length pattern.
   */
  @Test
  def variableLengthRelationships(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN*1..3]-(movie:Movie)
        |RETURN movie.title
        |""".stripMargin).records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(List("Wall Street", "The American President", "The American President"), records.toList)
  }

  /**
   * variable length relationships can be combined with multiple relationship types.
   * In this case the *minHops..maxHops applies to all relationship types as well as any combination of them.
   */
  @Test
  def variableLengthRelationshipsWithMultipleRelationshipTypes(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN|DIRECTED*2]-(person:Person)
        |RETURN person.name
        |""".stripMargin).records().map(f => f("person.name").asInstanceOf[LynxValue].value).toArray


    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Oliver Stone", "Michael Douglas", "Martin Sheen"), records.toSet)
  }


  @Test
  def relationshipVariableInVariableLengthRelationships(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH p = (actor {name: 'Charlie Sheen'})-[r:ACTED_IN*2]-(co_actor)
        |RETURN p
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, records.length)
    val link1 = records(0)("p").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]](1).value.asInstanceOf[List[LynxValue]](0).value.asInstanceOf[List[LynxValue]]
    val link2 = records(1)("p").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]](1).value.asInstanceOf[List[LynxValue]](0).value.asInstanceOf[List[LynxValue]]

    Assert.assertEquals(List(r4, r2), link1)
    Assert.assertEquals(List(r4, r5), link2)

    // should get:
    //[:ACTED_IN[0]{role:"Bud Fox"},:ACTED_IN[2]{role:"Gordon Gekko"}]
    //[:ACTED_IN[0]{role:"Bud Fox"},:ACTED_IN[1]{role:"Carl Fox"}]
  }

  @Test
  def matchWithPropertiesOnAVariableLengthPath(): Unit ={
    runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (martin:Person {name: 'Martin Sheen'})
        |CREATE (charlie)-[:X {blocked: false}]->(:UNBLOCKED)<-[:X {blocked: false}]-(martin)
        |CREATE (charlie)-[:X {blocked: true}]->(:BLOCKED)<-[:X {blocked: false}]-(martin)
        |""".stripMargin)

    val records = runOnDemoGraph(
      """
        |MATCH p = (charlie:Person)-[* {blocked:false}]-(martin:Person)
        |WHERE charlie.name = 'Charlie Sheen' AND martin.name = 'Martin Sheen'
        |RETURN p
        |""".stripMargin).records()

    Assert.assertEquals(1, records.length)
    //should get: (0)-[X,7]->(7)<-[X,8]-(1)
  }

  @Test
  def zeroLengthPath(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (wallstreet:Movie {title: 'Wall Street'})-[*0..1]-(x)
        |RETURN x
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
  }

  @Test
  def namedPaths(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH p = (michael {name: 'Michael Douglas'})-->()
        |RETURN p
        |""".stripMargin).records().map(f => f("p").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(2, records.length)
    Assert.assertEquals(List(n2, LynxList(List(r2, m1, LynxList(List())))), records.head)
    Assert.assertEquals(List(n2, LynxList(List(r3, m2, LynxList(List())))), records(1))
  }

  @Test
  def matchingOnABoundRelationship(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (a)-[r]-(b)
        |WHERE id(r) = 1
        |RETURN a, b
        |""".stripMargin).records().toArray

    Assert.assertEquals(2, records.length)
    Assert.assertEquals(n1, records.head("a"))
    Assert.assertEquals(m1, records.head("b"))
    Assert.assertEquals(m1, records(1)("a"))
    Assert.assertEquals(n1, records(1)("b"))
  }

  @Test
  def singleShortestPath(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH
        |  (martin:Person {name: 'Martin Sheen'}),
        |  (oliver:Person {name: 'Oliver Stone'}),
        |  p = shortestPath((martin)-[*..15]-(oliver))
        |RETURN p
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
  }

  @Test
  def allShortestPath(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH
        |  (martin:Person {name: 'Martin Sheen'} ),
        |  (michael:Person {name: 'Michael Douglas'}),
        |  p = allShortestPaths((martin)-[*]-(michael))
        |RETURN p
        |""".stripMargin).records().toArray

    Assert.assertEquals(2, records.length)
  }

  @Test
  def NodeById(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n) = 1
        |RETURN n
        |""".stripMargin).records().map(f => f("n").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(n1, records.head)
  }

  @Test
  def relationById(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH ()-[r]->()
        |WHERE id(r) = 1
        |RETURN r
        |""".stripMargin).records().map(f => f("r").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(r1, records.head)
  }

  @Test
  def multipleNodesById(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n) IN [1, 3, 5]
        |RETURN n
        |""".stripMargin).records().map(f => f("n").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(n1, records.head)
    Assert.assertEquals(n3, records(1))
    Assert.assertEquals(n5, records(2))
  }
}
