package org.grapheco.cypher

import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.{NodeInput, RelationshipInput, StoredNodeInputRef, TestBase, types}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:02
 */
class I_Limit extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("A")))
  val n2 = TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("B")))
  val n3 = TestNode(TestId(3), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("C")))
  val n4 = TestNode(TestId(4), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("D")))
  val n5 = TestNode(TestId(5), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("E")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(1), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(1), TestId(5), Option(LynxRelationshipType("KNOWS")), Map.empty)


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

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def returnALimitedSubsetOfTheRows(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name
        |ORDER BY n.name
        |LIMIT 3
        |""".stripMargin).records().toArray

    Assert.assertEquals(LynxString("A"), res(0)("n.name"))
    Assert.assertEquals(LynxString("B"), res(1)("n.name"))
    Assert.assertEquals(LynxString("C"), res(2)("n.name"))
  }

  @Test
  def usingAnExpressionWithLIMITToReturnASubsetOfTheRows(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name
        |ORDER BY n.name
        |LIMIT 1 + toInteger(3 * rand())
        |""".stripMargin).records().toArray
  }
  @Test
  def LIMITWillNotStopSideEffects(): Unit ={
    val num = nodesInput.length
    val res1 = runOnDemoGraph(
      """
        |CREATE (n)
        |RETURN n
        |LIMIT 0
        |""".stripMargin).records().toArray
    Assert.assertEquals(0, res1.length)

    val res2 = runOnDemoGraph(
      """
        |match (n) return n
        |""".stripMargin).records().toArray
    Assert.assertEquals(num + 1, res2.length)

    val res3 = runOnDemoGraph(
      """
        |MATCH (n)
        |WITH n LIMIT 1
        |SET n.locked = true
        |RETURN n
        |""".stripMargin).records().toArray

    val res4 = runOnDemoGraph(
      """
        |match (n) return n
        |""".stripMargin).records().toArray.count(f => f("n").asInstanceOf[LynxNode].property(LynxPropertyKey("locked")).isDefined)
    Assert.assertEquals(1, res4)
  }
}
