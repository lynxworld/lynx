package org.grapheco.costbased

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class PlanMatch extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Oliver Stone")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Michael Douglas")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Charlie Sheen")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Martin Sheen")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Rob Reiner")))
  val m1 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("Wall Street")))
  val m2 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title")-> LynxValue("The American President")))


  val r1 = TestRelationship(TestId(1), TestId(1), TestId(6), Option(LynxRelationshipType("DIRECTED")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(2), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("Gordon Gekko")))
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("President Andrew Shepherd")))
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(6), Option(LynxRelationshipType("WRITE")), Map(LynxPropertyKey("role")->LynxValue("Bud Fox")))
  val r5 = TestRelationship(TestId(5), TestId(4), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("Carl Fox")))
  val r6 = TestRelationship(TestId(6), TestId(4), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map(LynxPropertyKey("role")->LynxValue("A.J. MacInerney")))
  val r7 = TestRelationship(TestId(7), TestId(5), TestId(7), Option(LynxRelationshipType("DIRECTED")), Map.empty)


  @BeforeEach
  def init(): Unit ={
    all_nodes.clear()
    all_rels.clear()
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
    model.write.commit
  }

  @Test
  def singleNodeAll(): Unit = {
    val records = runOnDemoGraph("Match (n) Return n").records().map(f => f("n").asInstanceOf[TestNode]).toArray.sortBy(r => (r.id.value))
    Assertions.assertEquals(7, records.length)
    Assertions.assertEquals(n1, records(0))
    Assertions.assertEquals(n2, records(1))
    Assertions.assertEquals(n3, records(2))
    Assertions.assertEquals(n4, records(3))
    Assertions.assertEquals(n5, records(4))
    Assertions.assertEquals(m1, records(5))
    Assertions.assertEquals(m2, records(6))
  }

  @Test
  def singleNodeLabel(): Unit = {
    runOnDemoGraph("Match (n:Person) Return n")
  }

  @Test
  def singleNodeLabelAndProps(): Unit = {
    runOnDemoGraph("Match (n:Person{name:'Oliver Stone'}) Return n.name as name")
  }

  @Test
  def doubleNodeLabel(): Unit = {
    runOnDemoGraph("Match (n:Person:Movie) Return n.name as name")
  }

  @Test
  def _1Hop(): Unit = {
    runOnDemoGraph("Match (n:Person)-[r:ACTED_IN]->(m:Movie) Return n.name as name, m.title as title")
  }

  @Test
  def _1HopRelFirst(): Unit = {
    runOnDemoGraph("Match (n:Person)-[r:WRITE]->(m:Movie) Return n.name as name, m.title as title")
  }

  @Test
  def _1HopWithFilter(): Unit = {
    runOnDemoGraph(
      """Match (n:Person)-[r:ACTED_IN]->(m:Movie)
        |Where n.name = 'Oliver Stone' and m.title = 'Wall Street'
        |Return n.name as name, m.title as title""".stripMargin)
  }

  @Test
  def _2Hop(): Unit = {
    runOnDemoGraph(
      """Match (n:Person)-[r:ACTED_IN]->(m:Movie)<-[r2:DIRECTED]-(d:Person)
        |Return n.name as actors, m.title as movie, d.name as directors""".stripMargin)
  }

  @Test
  def _3Hop(): Unit = {
    runOnDemoGraph(
      """Match (n:Person)-[r:ACTED_IN]->(m:Movie)<-[r2:DIRECTED]-(d:Person),
        |(n)-[r3:ACTED_IN]->(m2:Movie)
        |where m <> m2
        |Return n.name as actors, m.title as movie, d.name as directors, m2.title as movie2""".stripMargin)
  }

  @Test
  def _4HopWithFilter(): Unit = {
    runOnDemoGraph(
      """Match (n:Person)-[r:ACTED_IN]->(m:Movie)<-[r2:DIRECTED]-(d:Person),
        |(n)-[r3:ACTED_IN]->(m2:Movie)<-[r4:ACTED_IN]-(n2:Person)
        |where m <> m2 AND n <> n2
        |Return n.name as actors, m.title as movie, d.name as directors, m2.title as movie2""".stripMargin)
  }

}
