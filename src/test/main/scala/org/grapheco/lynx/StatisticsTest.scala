package org.grapheco.lynx

import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

class StatisticsTest extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Charlie Sheen"), LynxPropertyKey("bornIn") -> LynxValue("New York"), LynxPropertyKey("chauffeurName") -> LynxValue("John Brown")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Oliver Stone"), LynxPropertyKey("bornIn") -> LynxValue("New York"), LynxPropertyKey("chauffeurName") -> LynxValue("Bill White")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Michael Douglas"), LynxPropertyKey("bornIn") -> LynxValue("New Jersey"), LynxPropertyKey("chauffeurName") -> LynxValue("John Brown")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Martin Sheen"), LynxPropertyKey("bornIn") -> LynxValue("Ohio"), LynxPropertyKey("chauffeurName") -> LynxValue("Bob Brown")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Rob Reiner"), LynxPropertyKey("bornIn") -> LynxValue("New York"), LynxPropertyKey("chauffeurName") -> LynxValue("John Brown")))
  val m1 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title") -> LynxValue("Wall Street")))
  val m2 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("title") -> LynxValue("The American President")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("FATHER")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r6 = TestRelationship(TestId(6), TestId(4), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r7 = TestRelationship(TestId(7), TestId(4), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r8 = TestRelationship(TestId(8), TestId(5), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)



  @Before
  def init(): Unit = {
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
    relationsInput.append(("r8", RelationshipInput(Seq(r8.relationType.get), Seq.empty, StoredNodeInputRef(r8.startNodeId), StoredNodeInputRef(r8.endNodeId))))


    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
    model.write.commit

  }

  @Test
  def apiTest(): Unit ={
    val stat = this.model.statistics
    Assert.assertEquals(7, stat.numNode)
    Assert.assertEquals(8, stat.numRelationship)
    Assert.assertEquals(5, stat.numNodeByLabel(LynxNodeLabel("Person")))
    Assert.assertEquals(2, stat.numNodeByLabel(LynxNodeLabel("Movie")))
  }

  @Test
  def nodeNum(): Unit ={
    Assert.assertEquals(LynxValue(all_nodes.size),
      runOnDemoGraph("match (n) return count(n)").records().map(_.getAsInt("count(n)")).toList.head.get)
  }

  @Test
  def nodeNumByLabel(): Unit ={
    Assert.assertEquals(LynxValue(5),
      runOnDemoGraph("match (n:Person) return count(n)").records().map(_.getAsInt("count(n)")).toList.head.get)
  }

  @Test
  def relNum(): Unit ={
    Assert.assertEquals(LynxValue(all_rels.size),
      runOnDemoGraph("match ()-[r]->() return count(r)").records().map(_.getAsInt("count(r)")).toList.head.get)
  }

  @Test
  def relNumByType(): Unit = {
    Assert.assertEquals(LynxValue(7),
      runOnDemoGraph("match ()-[r:ACTED_IN]->() return count(r)").records().map(_.getAsInt("count(r)")).toList.head.get)
    Assert.assertEquals(LynxValue(1),
      runOnDemoGraph("match ()-[r:FATHER]->() return count(r)").records().map(_.getAsInt("count(r)")).toList.head.get)

  }
}
