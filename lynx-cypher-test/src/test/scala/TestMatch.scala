import org.grapheco.lynx.{LynxList, LynxRelationship, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:53 上午 2021/6/17
 * @Modified By:
 */
class TestMatch {
  val n1 = TestNode(1, Array("Person"), ("name",LynxString("Oliver Stone")))
  val n2 = TestNode(2, Array("Person"), ("name",LynxString("Michael Douglas")))
  val n3 = TestNode(3, Array("Person"), ("name",LynxString("Charlie Sheen")))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Martin Sheen")))
  val n5 = TestNode(5, Array("Person"), ("name",LynxString("Rob Reiner")))
  val m1 = TestNode(6, Array("Movie"), ("title",LynxString("Wall Street")))
  val m2 = TestNode(7, Array("Movie"), ("title",LynxString("The American President")))

  val r1 = TestRelationship(1, 1, 6, Option("DIRECTED"))
  val r2 = TestRelationship(2, 2, 6, Option("ACTED_IN"), Map("role"->LynxString("Gordon Gekko")).toSeq:_*)
  val r3 = TestRelationship(3, 2, 7, Option("ACTED_IN"), Map("role"->LynxString("President Andrew Shepherd")).toSeq:_*)
  val r4 = TestRelationship(4, 3, 6, Option("ACTED_IN"), Map("role"->LynxString("Bud Fox")).toSeq:_*)
  val r5 = TestRelationship(5, 4, 6, Option("ACTED_IN"), Map("role"->LynxString("Carl Fox")).toSeq:_*)
  val r6 = TestRelationship(6, 4, 7, Option("ACTED_IN"), Map("role"->LynxString("A.J. MacInerney")).toSeq:_*)
  val r7 = TestRelationship(7, 5, 7, Option("DIRECTED"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, m1, m2)
  relsBuffer.append(r1, r2, r3, r4, r5, r6, r7)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def getAllNodes(): Unit ={
    val records = testBase.runOnDemoGraph("Match (n) Return n").records().map(f => f("n").asInstanceOf[TestNode]).toArray
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
    val records = testBase.runOnDemoGraph("match (movie:Movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals("Wall Street", records(0))
    Assert.assertEquals("The American President", records(1))
  }

  @Test
  def relatedNodes(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (director {name: 'Oliver Stone'})--(movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def matchWithLabels(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})--(movie:Movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def outgoingRelationships(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})-->(movie) return movie.title").records().map(f => f("movie.title").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Wall Street", records.head)
  }

  @Test
  def directedRelationshipsAndVariable(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (:Person {name: 'Oliver Stone'})-[r]->(movie) return type(r)").records().map(f => f("type(r)").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("DIRECTED", records.head)
  }

  @Test
  def matchOnRelationshipType(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (wallstreet:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(actor) return actor.name").records().map(f => f("actor.name").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Michael Douglas", "Martin Sheen", "Charlie Sheen"), records.toSet)
  }

  @Test
  def matchOnMultipleRelationshipTypes(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (wallstreet {title: 'Wall Street'})<-[:ACTED_IN|:DIRECTED]-(person) return person.name").records().map(f => f("person.name").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(4, records.length)
    Assert.assertEquals(Set("Michael Douglas", "Martin Sheen", "Charlie Sheen", "Oliver Stone"), records.toSet)
  }

  @Test
  def matchOnRelationshipTypeAndUseAVariable(): Unit ={
    val records = testBase.runOnDemoGraph("MATCH (wallstreet {title: 'Wall Street'})<-[r:ACTED_IN]-(actor) return r.role").records().map(f => f("r.role").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Gordon Gekko", "Carl Fox", "Bud Fox"), records.toSet)
  }

  @Test
  def relationshipTypesWithUncommonCharacters(): Unit ={
    testBase.runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (rob:Person {name: 'Rob Reiner'})
        |CREATE (rob)-[:`TYPE INCLUDING A SPACE`]->(charlie)
        |""".stripMargin)
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (n {name: 'Rob Reiner'})-[r:`TYPE INCLUDING A SPACE`]->()
        |RETURN type(r)
        |""".stripMargin).records().map(f => f("type(r)").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("TYPE INCLUDING A SPACE", records.head)
  }

  @Test
  def multipleRelationships(): Unit ={
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN|DIRECTED*2]-(person:Person)
        |RETURN person.name
        |""".stripMargin).records().map(f => f("person.name").asInstanceOf[LynxValue].value).toArray


    Assert.assertEquals(3, records.length)
    Assert.assertEquals(Set("Oliver Stone", "Michael Douglas", "Martin Sheen"), records.toSet)
  }


  @Test
  def relationshipVariableInVariableLengthRelationships(): Unit ={
    val records = testBase.runOnDemoGraph(
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
  def tmp(): Unit ={
    testBase.runOnDemoGraph(
      """
        |MATCH p = (actor {name: 'Charlie Sheen'})-[r:ACTED_IN*1..3]->(co_actor)
        |RETURN p
        |""".stripMargin)
  }

  @Test
  def matchWithPropertiesOnAVariableLengthPath(): Unit ={
    testBase.runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (martin:Person {name: 'Martin Sheen'})
        |CREATE (charlie)-[:X {blocked: false}]->(:UNBLOCKED)<-[:X {blocked: false}]-(martin)
        |CREATE (charlie)-[:X {blocked: true}]->(:BLOCKED)<-[:X {blocked: false}]-(martin)
        |""".stripMargin)

    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
      """
        |MATCH (wallstreet:Movie {title: 'Wall Street'})-[*0..1]-(x)
        |RETURN x
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
  }

  @Test
  def namedPaths(): Unit ={
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
    val records = testBase.runOnDemoGraph(
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
