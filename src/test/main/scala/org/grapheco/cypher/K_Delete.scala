package org.grapheco.cypher

import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{TestBase, types}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:11
 */
class K_Delete extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Andy"), LynxPropertyKey("age")->LynxValue(36)))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("UNKNOWN")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Timothy"), LynxPropertyKey("age")-> LynxValue(25)))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Peter"), LynxPropertyKey("age")-> LynxValue(34)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)

  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def deleteSingleNode(): Unit ={
    val num = nodesInput.length
    runOnDemoGraph(
      """
        |MATCH (n:Person {name: 'UNKNOWN'})
        |DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, all_nodes.size)
  }

  @Test
  def deleteAllNodesAndRelationships(): Unit ={
    runOnDemoGraph(
      """
        |MATCH (n)
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(0, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def deleteANodeWithAllItsRelationships(): Unit ={
    val num = nodesInput.length

    runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def deleteRelationshipsOnly(): Unit ={
    val num = nodesInput.length

    runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})-[r:KNOWS]->()
        |DELETE r
        |""".stripMargin)

    Assert.assertEquals(num, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }
}
