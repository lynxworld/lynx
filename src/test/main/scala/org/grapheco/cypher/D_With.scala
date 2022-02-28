package org.grapheco.cypher

import org.grapheco.lynx.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, LynxString, LynxValue, NodeInput, RelationshipInput, StoredNodeInputRef, TestBase}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 16:46
 */
class D_With extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("Anders")))
  val n2 = TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("Caesar")))
  val n3 = TestNode(TestId(3), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("Bossman")))
  val n4 = TestNode(TestId(4), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("George")))
  val n5 = TestNode(TestId(5), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("David")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("BLOCKS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("BLOCKS")), Map.empty)
  val r6 = TestRelationship(TestId(6), TestId(5), TestId(1), Option(LynxRelationshipType("KNOWS")), Map.empty)

  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), r4.props.toSeq, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), r5.props.toSeq, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))
    relationsInput.append(("r6", RelationshipInput(Seq(r6.relationType.get), r6.props.toSeq, StoredNodeInputRef(r6.startNodeId), StoredNodeInputRef(r6.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }
  @Test
  def filterOnAggregateFunctionResults(): Unit ={
    runOnDemoGraph(
      """
        |MATCH (david {name: 'David'})--(otherPerson)-->()
        |WITH otherPerson, count(*) AS foaf
        |WHERE foaf > 1
        |RETURN otherPerson.name
        |""".stripMargin)
  }

  @Test
  def sortResultsBeforeUsingCollectOnThem(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |WITH n
        |ORDER BY n.name DESC
        |LIMIT 3
        |RETURN collect(n.name)
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("George","David","Caesar").map(LynxString), res(0)("collect(n.name)").asInstanceOf[LynxValue].value)
  }

  @Test
  def limitBranchingOfAPathSearch(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n {name: 'Anders'})--(m)
        |WITH m
        |ORDER BY m.name DESC
        |LIMIT 1
        |MATCH (m)--(o)
        |RETURN o.name
        |""".stripMargin).records().toArray

    Assert.assertEquals("Bossman", res(0)("o.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Anders", res(1)("o.name").asInstanceOf[LynxValue].value)
  }
}
