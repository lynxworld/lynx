package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 19:00
 */
class R_Union extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Actor")), Map(LynxPropertyKey("name")-> LynxValue("Anthony Hopkins")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Actor")), Map(LynxPropertyKey("name")-> LynxValue("Hitchcock")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Actor")), Map(LynxPropertyKey("name")-> LynxValue("Helen Mirren"), LynxPropertyKey("age")-> LynxValue(36), LynxPropertyKey("hungry")->LynxValue(true)))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("name")-> LynxValue("Hitchcock"), LynxPropertyKey("age")-> LynxValue(34)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("ACTS_IN")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(3), TestId(4), Option(LynxRelationshipType("ACTS_IN")), Map.empty)

  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))



    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))



    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
    model.write.commit
  }

  @Test
  def combineTwoQueriesAndRetainDuplicates(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION ALL
        |MATCH (n:Movie)
        |RETURN n.name AS name
        |""".stripMargin).records().toArray.map(_.getAsString("name").get.v).sorted

    Assert.assertEquals("Anthony Hopkins", res(0))
    Assert.assertEquals("Helen Mirren", res(1))
    Assert.assertEquals("Hitchcock", res(2))
    Assert.assertEquals("Hitchcock", res(3))

  }

  @Test
  def combineTwoQueriesAndRemoveDuplicates(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION
        |MATCH (n:Movie)
        |RETURN n.name AS name
        |""".stripMargin).records().toArray.map(_.getAsString("name").get.v).sorted

    Assert.assertEquals("Anthony Hopkins", res(0))
    Assert.assertEquals("Helen Mirren", res(1))
    Assert.assertEquals("Hitchcock", res(2))
  }
}
