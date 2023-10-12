package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 16:14
 */
class P_Call_skip extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person"), LynxNodeLabel("Child")), Map(LynxPropertyKey("name")-> LynxValue("Alice"), LynxPropertyKey("age") -> LynxValue(20)))
  val n2 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Dora"), LynxPropertyKey("age") -> LynxValue(30)))
  val n3 = TestNode(TestId(1), Seq(LynxNodeLabel("Person"), LynxNodeLabel("Parent")), Map(LynxPropertyKey("name")-> LynxValue("Charlie"), LynxPropertyKey("age") -> LynxValue(65)))
  val n4 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Bob"), LynxPropertyKey("age") -> LynxValue(27)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("CHILD_OF")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("FRIEND_OF")), Map.empty)


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
    model.write.commit
  }
  @Test
  def importingVariablesIntoSubqueries(): Unit ={
    val res = runOnDemoGraph(
      """
        |UNWIND [0, 1, 2] AS x
        |CALL {
        |  WITH x
        |  RETURN x * 10 AS y
        |}
        |RETURN x, y
        |""".stripMargin)
  }

  @Test
  def postUnionProcessing(): Unit ={
    val res = runOnDemoGraph(
      """
        |CALL {
        |  MATCH (p:Person)
        |  RETURN p
        |  ORDER BY p.age ASC
        |  LIMIT 1
        |UNION
        |  MATCH (p:Person)
        |  RETURN p
        |  ORDER BY p.age DESC
        |  LIMIT 1
        |}
        |RETURN p.name, p.age
        |ORDER BY p.name
        |""".stripMargin)
  }
}
