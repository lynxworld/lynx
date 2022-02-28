package org.grapheco.cypher

import org.grapheco.lynx.{LynxInteger, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, LynxString, LynxValue, NodeInput, RelationshipInput, StoredNodeInputRef, TestBase}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:31
 */
class O_Merge extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Charlie Sheen"), LynxPropertyKey("bornIn")->LynxValue("New York"), LynxPropertyKey("chauffeurName")->LynxValue("John Brown")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Oliver Stone"), LynxPropertyKey("bornIn")->LynxValue("New York"), LynxPropertyKey("chauffeurName")->LynxValue("Bill White")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Michael Douglas"), LynxPropertyKey("bornIn")->LynxValue("New Jersey"), LynxPropertyKey("chauffeurName")->LynxValue("John Brown")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Martin Sheen"), LynxPropertyKey("bornIn")->LynxValue("Ohio"), LynxPropertyKey("chauffeurName")->LynxValue("Bob Brown")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Rob Reiner"), LynxPropertyKey("bornIn")->LynxValue("New York"), LynxPropertyKey("chauffeurName")->LynxValue("John Brown")))
  val m1 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("Wall Street")))
  val m2 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("The American President")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("FATHER")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r6 = TestRelationship(TestId(6), TestId(4), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r7 = TestRelationship(TestId(7), TestId(4), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r8 = TestRelationship(TestId(8), TestId(5), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)


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
    relationsInput.append(("r8", RelationshipInput(Seq(r8.relationType.get), Seq.empty, StoredNodeInputRef(r8.startNodeId), StoredNodeInputRef(r8.endNodeId))))


    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def testMergeOnCreate(): Unit ={
    val nodeNum = nodesInput.length

    val records = runOnDemoGraph(
      """
        |MERGE (keanu:Person {name: 'Keanu Reeves'})
        |ON CREATE
        |  SET keanu.created = timestamp()
        |RETURN keanu.name, keanu.created
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(nodeNum + 1, all_nodes.length)
  }

  @Test
  def testMergeOnMatch(): Unit ={
    val nodeNum = nodesInput.length

    val records = runOnDemoGraph(
      """
        |MERGE (person:Person)
        |ON MATCH
        |  SET person.found = true
        |RETURN person.name, person.found
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    Assert.assertEquals(nodeNum, all_nodes.length)
  }

  @Test
  def testMergeWithOnCreateOnMatch(): Unit ={
    val nodeNum = nodesInput.length

    val records = runOnDemoGraph(
      """
        |MERGE (keanu:Person {name: 'Keanu Reeves'})
        |ON CREATE
        |  SET keanu.created = timestamp()
        |ON MATCH
        |  SET keanu.lastSeen = timestamp()
        |RETURN keanu.name, keanu.created, keanu.lastSeen
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(nodeNum + 1, all_nodes.length)
    Assert.assertEquals(null, records.head("keanu.lastSeen").asInstanceOf[LynxValue].value)
  }

  @Test
  def testMergeOnMatchSettingMultipleProperties(): Unit ={
    val nodeNum = nodesInput.length

    val records = runOnDemoGraph(
      """
        |MERGE (person:Person)
        |ON MATCH
        |  SET
        |    person.found = true,
        |    person.lastAccessed = timestamp()
        |RETURN person.name, person.found, person.lastAccessed
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    Assert.assertEquals(nodeNum, all_nodes.length)
  }

  @Test
  def testMergeSingleNodeWithALabel(): Unit ={
    val nodeNum = nodesInput.length
    val records = runOnDemoGraph(
      """
        |MERGE (robert:Critic)
        |RETURN robert, labels(robert)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(nodeNum + 1, all_nodes.length)
    Assert.assertEquals(TestNode(TestId(nodeNum + 1), Seq(LynxNodeLabel("Critic")), Map.empty), records.head("robert").asInstanceOf[LynxNode])
    Assert.assertEquals(List("Critic"), records.head("labels(robert)").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }

  @Test
  def testMergeSingleNodeWithProperty(): Unit = {
    val nodeNum = nodesInput.length
    val records = runOnDemoGraph(
      """
        |MERGE (charlie {name: 'Charlie Sheen', age: 10})
        |RETURN charlie
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(nodeNum + 1, all_nodes.length)
    Assert.assertEquals(TestNode(TestId(nodeNum + 1), Seq.empty, Seq(LynxPropertyKey("name")->LynxString("Charlie Sheen"), LynxPropertyKey("age")->LynxInteger(10)).toMap), records.head("charlie"))

  }

  @Test
  def testMergeSingleNodeWithLabelAndProperty(): Unit = {
    val nodeNum = nodesInput.length
    val records = runOnDemoGraph(
      """
        |MERGE (michael:Person {name: 'Michael Douglas'})
        |RETURN michael.name, michael.bornIn
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(nodeNum, all_nodes.length)
    Assert.assertEquals(n3.property(LynxPropertyKey("name")).get, records.head("michael.name"))
    Assert.assertEquals(n3.property(LynxPropertyKey("bornIn")).get, records.head("michael.bornIn"))
  }

  @Test
  def testMergeSingleNodeDerivedFromExistingNodeProperty(): Unit = {
    val nodeNum = nodesInput.length
    val records1 = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |RETURN person.name, person.bornIn, city
        |""".stripMargin).records().toArray

    val records2 = runOnDemoGraph("match (n:City) return n").records().toArray

    Assert.assertEquals(5, records1.length)
    Assert.assertEquals(3, records2.length)
    Assert.assertEquals(nodeNum + 3, all_nodes.length)
    Assert.assertEquals(Seq("New York", "New Jersey", "Ohio"), records2.map(f => f("n").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value).toSeq)
  }

  @Test
  def testMergeOnAExistRelationship(): Unit = {
    val relNum = nodesInput.size

    val records = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
        |RETURN charlie.name, type(r),id(r),wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum, all_nodes.size)
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), records.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_IN"), records.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), records.head("wallStreet.title"))
  }

  @Test
  def testMergeOnANonExistRelationship(): Unit = {
    val relNum = relationsInput.size
    val nodesNum = nodesInput.size

    val records = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (wallStreet:Movie {title: 'Wall Street'})
        |MERGE (charlie)-[r:ACTED_XXX]->(wallStreet)
        |RETURN charlie.name, type(r), wallStreet.title
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, all_rels.size)
    Assert.assertEquals(nodesNum, all_nodes.size)
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxString("Charlie Sheen"), records.head("charlie.name"))
    Assert.assertEquals(LynxString("ACTED_XXX"), records.head("type(r)"))
    Assert.assertEquals(LynxString("Wall Street"), records.head("wallStreet.title"))
  }

  @Test
  def testMergeOnMultipleRelationship(): Unit = {
    val nodesNum = nodesInput.size
    val relNum = relationsInput.size
    val records = runOnDemoGraph(
      """
        |MATCH
        |  (oliver:Person {name: 'Oliver Stone'}),
        |  (reiner:Person {name: 'Rob Reiner'})
        |MERGE (oliver)-[r1:DIRECTED]->(movie:Movie)<-[r2:ACTED_IN]-(reiner)
        |RETURN movie
        |""".stripMargin).records().toArray

    val movie = TestNode(TestId(nodesNum + 1), Seq(LynxNodeLabel("Movie")), Map.empty)

    Assert.assertEquals(nodesNum + 1, all_nodes.length)
    Assert.assertEquals(relNum + 2, all_rels.length)
    Assert.assertEquals(movie, records.head("movie"))
    Assert.assertEquals(
      TestRelationship(TestId(relNum + 1), TestId(n2.id.value), TestId(movie.id.value), Option(LynxRelationshipType("DIRECTED")), Map.empty),
      all_rels(relNum)
    )
    Assert.assertEquals(
      TestRelationship(TestId(relNum + 2), TestId(n5.id.value), TestId(movie.id.value), Option(LynxRelationshipType("ACTED_IN")), Map.empty),
      all_rels(relNum + 1)
    )
  }

  @Test
  def testMergeOnAUndirectedRelationship(): Unit ={
    val relNum = relationsInput.size
    val nodesNum = nodesInput.size

    val records = runOnDemoGraph(
      """
        |MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (oliver:Person {name: 'Oliver Stone'})
        |MERGE (charlie)-[r:KNOWS]-(oliver)
        |RETURN r
        |""".stripMargin).records().toArray

    Assert.assertEquals(relNum + 1, all_rels.length)
    Assert.assertEquals(nodesNum, all_nodes.length)
    Assert.assertEquals("KNOWS", records.head("r").asInstanceOf[LynxRelationship].relationType.get)
  }

  @Test
  def testMergeOnARelationshipBetweenTwoExistingNodes(): Unit ={
    val relNum = relationsInput.size
    val nodesNum = nodesInput.size

    val records = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (city:City {name: person.bornIn})
        |MERGE (person)-[r:BORN_IN]->(city)
        |RETURN person.name, person.bornIn, city
        |""".stripMargin).records().toArray

    Assert.assertEquals(nodesNum + 3, all_nodes.length)
    Assert.assertEquals(relNum + 5, all_rels.length)
  }

  @Test
  def testMergeOnARelationshipBetweenAnExistingNodeAndAMergedNodeDerivedFromANodeProperty(): Unit ={
    val relNum = relationsInput.size
    val nodesNum = nodesInput.size

    val records = runOnDemoGraph(
      """
        |MATCH (person:Person)
        |MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
        |RETURN person.name, person.chauffeurName, chauffeur
        |""".stripMargin).records().toArray

    Assert.assertEquals(nodesNum + 5, all_nodes.length)
    Assert.assertEquals(relNum + 5, all_rels.length)
  }
}
