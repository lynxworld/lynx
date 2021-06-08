package org.grapheco.lynx

import org.junit.{Assert, Before, Test}

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2021-05-25 14:47
 */
class CypherMergeTest extends TestBase {

  var n1: TestNode = _
  var n2: TestNode = _
  var n3: TestNode = _
  var n4: TestNode = _
  var n5: TestNode = _

  @Before
  def initData(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    n1 = TestNode(1, Seq("Person"), "bornIn"->LynxValue("New York"), "name"->LynxValue("Oliver Stone"), "chauffeurName"->LynxValue("Bill White"))
    n2 = TestNode(2, Seq("Person"), "bornIn"->LynxValue("New Jersey"), "name"->LynxValue("Michael Douglas"), "chauffeurName"->LynxValue("John Brown"))
    n3 = TestNode(3, Seq("Person"), "bornIn"->LynxValue("Ohio"), "name"->LynxValue("Martin Sheen"), "chauffeurName"->LynxValue("Bob Brown"))
    n4 = TestNode(4, Seq("Person"), "bornIn"->LynxValue("New York"), "name"->LynxValue("Rob Reiner"), "chauffeurName"->LynxValue("Ted Green"))
    n5 = TestNode(5, Seq("Person"), "bornIn"->LynxValue("New York"), "name"->LynxValue("Charlie Sheen"), "chauffeurName"->LynxValue("John Brown"))

    val m1 = TestNode(6, Seq("Movie"), "title"->LynxValue("Wall Street"))
    val m2 = TestNode(7, Seq("Movie"), "title"->LynxValue("The American President"))

    val r1 = TestRelationship(1, 5, 3, Option("Father"))
    val r2 = TestRelationship(2, 1, 6, Option("ACTED_IN"))
    val r3 = TestRelationship(3, 2, 6, Option("ACTED_IN"))
    val r4 = TestRelationship(4, 2, 7, Option("ACTED_IN"))
    val r5 = TestRelationship(5, 3, 6, Option("ACTED_IN"))
    val r6 = TestRelationship(6, 3, 7, Option("ACTED_IN"))
    val r7 = TestRelationship(7, 4, 7, Option("ACTED_IN"))
    val r8 = TestRelationship(8, 5, 6, Option("ACTED_IN"))
    val r9 = TestRelationship(9, 5, 6, Option("ACTED_IN"))

    all_nodes.append(n1, n2, n3, n4, n5, m1, m2)
    all_rels.append(r1, r2, r3, r4, r5, r6, r7, r8, r9)
  }

  @Test
  def testMergeSingleNodeWithLabel(): Unit = {
    val res = runOnDemoGraph(
      """
        |MERGE (robert:Critic)
        |RETURN robert, labels(robert)
        |""".stripMargin).records().next()

    Assert.assertEquals(TestNode(all_nodes.size, Seq("Critic")), res("robert"))
    Assert.assertEquals(Seq(LynxString("Critic")), res("labels(robert)").asInstanceOf[LynxValue].value)
  }

  @Test
  def testMergeSingleNodeWithProperty(): Unit = {
    val res = runOnDemoGraph(
      """
        |MERGE (charlie {name: 'Charlie Sheen', age: 10})
        |RETURN charlie
        |""".stripMargin).records().next()

    Assert.assertEquals(TestNode(all_nodes.size, Seq.empty, "name"->LynxString("Charlie Sheen"), "age"->LynxInteger(10)), res("charlie"))
  }

  @Test
  def testMergeSingleNodeWithLabelAndProperty(): Unit = {
    val res = runOnDemoGraph(
      """
        |MERGE (michael:Person {name: 'Michael Douglas'})
        |RETURN michael.name, michael.bornIn
        |""".stripMargin).records().next()

    Assert.assertEquals(n2.property("name").get, res("michael.name"))
    Assert.assertEquals(n2.property("bornIn").get, res("michael.bornIn"))
  }

  @Test
  def testMergeSingleNodeDerivedFromExistingNodeProperty(): Unit = {
    val res1 = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |RETURN person.name, person.bornIn, city
        |""".stripMargin).records().toArray

    val res2 = runOnDemoGraph("match (n:City) return n").records().toArray

    Assert.assertEquals(5, res1.length)
    Assert.assertEquals(3, res2.length)
    Assert.assertEquals(Seq("New York", "New Jersey", "Ohio"),res2.map(f => f("n").asInstanceOf[LynxNode].property("name").get.value).toSeq)
  }


  @Test
  def testMergeOnAExistRelationship(): Unit = {
    val relNum = all_rels.size

    val res = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
        |RETURN charlie.name, type(r),id(r),wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum, all_rels.size)
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), res.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_IN"), res.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), res.head("wallStreet.title"))
  }

  @Test
  def testMergeOnANonExistRelationship(): Unit = {
    val relNum = all_rels.size
    val nodesNum = all_nodes.size

    val res = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_XXX]->(wallStreet)
        |RETURN charlie.name, type(r), wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, all_rels.size)
    Assert.assertEquals(nodesNum, all_nodes.size)
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), res.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_XXX"), res.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), res.head("wallStreet.title"))
  }

  @Test
  def testMergeOnMultipleRelationship(): Unit = {
    val nodesNum = all_nodes.size
    val relNum = all_rels.size
    val res = runOnDemoGraph(
      """
        |MATCH
        |  (oliver:Person {name: 'Oliver Stone'}),
        |  (reiner:Person {name: 'Rob Reiner'})
        |MERGE (oliver)-[r1:DIRECTED]->(movie:Movie)<-[r2:ACTED_IN]-(reiner)
        |RETURN movie
        |""".stripMargin).records().toArray

    Assert.assertEquals(nodesNum + 1, all_nodes.length)
    Assert.assertEquals(relNum + 2, all_rels.length)
    Assert.assertEquals(Seq("Movie"), res.head("movie").asInstanceOf[LynxNode].labels)
  }

  @Test
  def testMergeOnAUndirectedRelationship(): Unit ={
    val relNum = all_rels.size
    val nodesNum = all_nodes.size

    val res = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (oliver:Person {name: 'Oliver Stone'})
        |MERGE (charlie)-[r:KNOWS]-(oliver)
        |RETURN r
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, all_rels.length)
    Assert.assertEquals(nodesNum, all_nodes.length)
    Assert.assertEquals("KNOWS", res.head("r").asInstanceOf[LynxRelationship].relationType.get)
  }

  @Test
  def testMergeOnARelationshipBetweenTwoExistingNodes(): Unit ={
    val relNum = all_rels.size
    val nodesNum = all_nodes.size

    val res = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |MERGE (person)-[r:BORN_IN]->(city)
        |RETURN person.name, person.bornIn, city
        |""".stripMargin)
  }

  @Test
  def testMergeOnARelationshipBetweenAnExistingNodeAndAMergedNodeDerivedFromANodeProperty(): Unit ={
    val relNum = all_rels.size
    val nodesNum = all_nodes.size

    val res = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
        |RETURN person.name, person.chauffeurName, chauffeur
        |""".stripMargin)
  }
}
