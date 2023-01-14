package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

class Patterns extends TestBase {
  val nodeInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipInput = ArrayBuffer[(String, RelationshipInput)]()

  val nodes = List(
    TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("Anders"))),
    TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("Becky"))),
    TestNode(TestId(3), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("Cesar"))),
    TestNode(TestId(4), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("Dilshad"))),
    TestNode(TestId(5), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("Filipa"))),
    TestNode(TestId(6), Seq.empty, Map(LynxPropertyKey("name") -> LynxValue("George")))
  )

  val rels = List(
    TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty),
    TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty),
    TestRelationship(TestId(3), TestId(1), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty),
    TestRelationship(TestId(4), TestId(4), TestId(5), Option(LynxRelationshipType("KNOWS")), Map.empty),
    TestRelationship(TestId(5), TestId(2), TestId(6), Option(LynxRelationshipType("KNOWS")), Map.empty),
    TestRelationship(TestId(6), TestId(3), TestId(6), Option(LynxRelationshipType("KNOWS")), Map.empty),
  )

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    nodes.foreach(node => {
      nodeInput.append(("node" + node.id.value, NodeInput(node.labels, node.props.toSeq)))
    })

    rels.foreach(rel => {
      relationshipInput.append(("relation" + rel.id.value, RelationshipInput(Seq(rel.relationType.get), Seq.empty, StoredNodeInputRef(rel.startNodeId), StoredNodeInputRef(rel.endNodeId))))
    })

    model.write.createElements(nodeInput, relationshipInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      })
  }

  @Test
  def patternMatch():Unit={
    val records = runOnDemoGraph(
      """
        |MATCH (me)-[:KNOWS*1..2]-(remote_friend)
        |WHERE me.name = 'Filipa'
        |RETURN remote_friend.name
        |""".stripMargin).records().map(f=>f("remote_friend.name").value).toArray
    val expectResult = Array("Dilshad","Anders")
    compareArray(expectResult,records)
  }

  /**
   * compare expectation Result with actual Result
   *
   * @param expectResult
   * @param records
   * @tparam A
   */
  def compareArray[A](expectResult: Array[A], records: Array[Any]): Unit = {
    Assert.assertEquals(expectResult.length, records.length)
    for (i <- 0 to records.length - 1) {
      Assert.assertEquals(expectResult(i), records(i))
    }
  }
}
