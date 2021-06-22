import org.grapheco.lynx.{LynxInteger, LynxNode, LynxRelationship, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-18 09:09
 */
class TestMerge {
  val n3 = TestNode(1, Array("Person"), ("name",LynxString("Charlie Sheen")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("John Brown")))
  val n1 = TestNode(2, Array("Person"), ("name",LynxString("Oliver Stone")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("Bill White")))
  val n2 = TestNode(3, Array("Person"), ("name",LynxString("Michael Douglas")), ("bornIn", LynxString("New Jersey")), ("chauffeurName", LynxString("John Brown")))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Martin Sheen")), ("bornIn", LynxString("Ohio")), ("chauffeurName", LynxString("Bob Brown")))
  val n5 = TestNode(5, Array("Person"), ("name",LynxString("Rob Reiner")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("Ted Green")))
  val m1 = TestNode(6, Array("Movie"), ("title",LynxString("Wall Street")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("John Brown")))
  val m2 = TestNode(7, Array("Movie"), ("title",LynxString("The American President")))

  val r1 = TestRelationship(1, 1, 6, Option("ACTED_IN"))
  val r2 = TestRelationship(2, 1, 4, Option("FATHER"))
  val r3 = TestRelationship(3, 2, 6, Option("ACTED_IN"))
  val r4 = TestRelationship(4, 3, 6, Option("ACTED_IN"))
  val r5 = TestRelationship(5, 3, 7, Option("ACTED_IN"))
  val r6 = TestRelationship(6, 4, 6, Option("ACTED_IN"))
  val r7 = TestRelationship(7, 4, 7, Option("ACTED_IN"))
  val r8 = TestRelationship(8, 5, 7, Option("ACTED_IN"))



  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, m1, m2)
  relsBuffer.append(r1, r2, r3, r4, r5, r6, r7, r8)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def mergeSingleNodeWithALabel(): Unit ={
    val records = testBase.runOnDemoGraph(
      """
        |MERGE (robert:Critic)
        |RETURN robert, labels(robert)
        |""".stripMargin)
  }

  @Test
  def testMergeSingleNodeWithLabel(): Unit = {
    val res = testBase.runOnDemoGraph(
      """
        |MERGE (robert:Critic)
        |RETURN robert, labels(robert)
        |""".stripMargin).records().next()

    Assert.assertEquals(TestNode(nodesBuffer.size, Seq("Critic")), res("robert"))
    Assert.assertEquals(Seq(LynxString("Critic")), res("labels(robert)").asInstanceOf[LynxValue].value)
  }

  @Test
  def testMergeSingleNodeWithProperty(): Unit = {
    val res = testBase.runOnDemoGraph(
      """
        |MERGE (charlie {name: 'Charlie Sheen', age: 10})
        |RETURN charlie
        |""".stripMargin).records().next()

    Assert.assertEquals(TestNode(nodesBuffer.size, Seq.empty, "name"->LynxString("Charlie Sheen"), "age"->LynxInteger(10)), res("charlie"))
  }

  @Test
  def testMergeSingleNodeWithLabelAndProperty(): Unit = {
    val res = testBase.runOnDemoGraph(
      """
        |MERGE (michael:Person {name: 'Michael Douglas'})
        |RETURN michael.name, michael.bornIn
        |""".stripMargin).records().next()

    Assert.assertEquals(n2.property("name").get, res("michael.name"))
    Assert.assertEquals(n2.property("bornIn").get, res("michael.bornIn"))
  }

  @Test
  def testMergeSingleNodeDerivedFromExistingNodeProperty(): Unit = {
    val res1 = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |RETURN person.name, person.bornIn, city
        |""".stripMargin).records().toArray

    val res2 = testBase.runOnDemoGraph("match (n:City) return n").records().toArray

    Assert.assertEquals(5, res1.length)
    Assert.assertEquals(3, res2.length)
    Assert.assertEquals(Seq("New York", "New Jersey", "Ohio"),res2.map(f => f("n").asInstanceOf[LynxNode].property("name").get.value).toSeq)
  }


  @Test
  def testMergeOnAExistRelationship(): Unit = {
    val relNum = nodesBuffer.size

    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
        |RETURN charlie.name, type(r),id(r),wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum, nodesBuffer.size)
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), res.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_IN"), res.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), res.head("wallStreet.title"))
  }

  @Test
  def testMergeOnANonExistRelationship(): Unit = {
    val relNum = relsBuffer.size
    val nodesNum = nodesBuffer.size

    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_XXX]->(wallStreet)
        |RETURN charlie.name, type(r), wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, relsBuffer.size)
    Assert.assertEquals(nodesNum, nodesBuffer.size)
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), res.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_XXX"), res.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), res.head("wallStreet.title"))
  }

  @Test
  def testMergeOnMultipleRelationship(): Unit = {
    val nodesNum = nodesBuffer.size
    val relNum = relsBuffer.size
    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (oliver:Person {name: 'Oliver Stone'}),
        |  (reiner:Person {name: 'Rob Reiner'})
        |MERGE (oliver)-[r1:DIRECTED]->(movie:Movie)<-[r2:ACTED_IN]-(reiner)
        |RETURN movie
        |""".stripMargin).records().toArray

    Assert.assertEquals(nodesNum + 1, nodesBuffer.length)
    Assert.assertEquals(relNum + 2, relsBuffer.length)
    Assert.assertEquals(Seq("Movie"), res.head("movie").asInstanceOf[LynxNode].labels)
  }

  @Test
  def testMergeOnAUndirectedRelationship(): Unit ={
    val relNum = relsBuffer.size
    val nodesNum = nodesBuffer.size

    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (oliver:Person {name: 'Oliver Stone'})
        |MERGE (charlie)-[r:KNOWS]-(oliver)
        |RETURN r
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, relsBuffer.length)
    Assert.assertEquals(nodesNum, nodesBuffer.length)
    Assert.assertEquals("KNOWS", res.head("r").asInstanceOf[LynxRelationship].relationType.get)
  }

  @Test
  def testMergeOnARelationshipBetweenTwoExistingNodes(): Unit ={
    val relNum = relsBuffer.size
    val nodesNum = nodesBuffer.size

    val res = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |MERGE (person)-[r:BORN_IN]->(city)
        |RETURN person.name, person.bornIn, city
        |""".stripMargin)
  }

  @Test
  def testMergeOnARelationshipBetweenAnExistingNodeAndAMergedNodeDerivedFromANodeProperty(): Unit ={
    val relNum = relsBuffer.size
    val nodesNum = nodesBuffer.size

    val res = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
        |RETURN person.name, person.chauffeurName, chauffeur
        |""".stripMargin)
  }
}
