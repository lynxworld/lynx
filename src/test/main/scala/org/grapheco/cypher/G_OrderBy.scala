package org.grapheco.cypher

import org.grapheco.lynx.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, LynxValue, NodeInput, RelationshipInput, StoredNodeInputRef, TestBase}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 17:54
 */
class G_OrderBy extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("A"), LynxPropertyKey("age")->LynxValue(34), LynxPropertyKey("length")->LynxValue(170)))
  val n2 = TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("B"), LynxPropertyKey("age")->LynxValue(36)))
  val n3 = TestNode(TestId(3), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("C"), LynxPropertyKey("age")->LynxValue(32), LynxPropertyKey("length")->LynxValue(180)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("BLOCKS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)


  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def orderNodesByProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.name
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("A", 34), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("C", 32), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderNodesByMultipleProperties(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.age, n.name
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", 32), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("A", 34), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderNodesInDescendingOrder(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name, n.age
        |ORDER BY n.name DESC
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", 32), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("B", 36), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("A", 34), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderingNull(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.length, n.name, n.age
        |ORDER BY n.length
        |""".stripMargin).records().toArray

    Assert.assertEquals(List(170, "A", 34), List(res(0)("n.length"), res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List(180, "C", 32), List(res(1)("n.length"), res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List(null, "B", 36), List(res(2)("n.length"), res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def orderingInAWITHClause(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |WITH n ORDER BY n.age
        |RETURN collect(n.name) AS names
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("C", "A", "B").map(LynxValue(_)), res(0)("names").asInstanceOf[LynxValue].value)
  }
}
