package org.grapheco.cypher

import org.grapheco.lynx.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, LynxValue, NodeInput, RelationshipInput, StoredNodeInputRef, TestBase}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 15:52
 */
class E_Unwind extends TestBase{
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
  def unwindAList(): Unit = {
    val res = runOnDemoGraph(
      """
        |UNWIND [1, 2, 3, null] AS x
        |RETURN x, 'val' AS y
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(3)("x").asInstanceOf[LynxValue].value)

    Assert.assertEquals("val", res(0)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(1)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(2)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(3)("y").asInstanceOf[LynxValue].value)
  }

  @Test
  def creatingADistinctList(): Unit ={
    val res = runOnDemoGraph(
      """
        |WITH [1, 1, 2, 2] AS coll
        |UNWIND coll AS x
        |WITH DISTINCT x
        |RETURN collect(x) AS setOfVals
        |""".stripMargin).records().toArray
    Assert.assertEquals(List(1, 2).map(LynxValue(_)), res(0)("setOfVals").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithAnyExpressionReturningAList(): Unit ={
    val res = runOnDemoGraph(
      """
        |WITH [1, 2] AS a,[3, 4] AS b
        |UNWIND (a + b) AS x
        |RETURN x
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(4, res(3)("x").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithAListOfLists(): Unit ={
    val res = runOnDemoGraph(
      """
        |WITH [[1, 2],[3, 4], 5] AS nested
        |UNWIND nested AS x
        |UNWIND x AS y
        |RETURN y
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res(0)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(4, res(3)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(5, res(4)("y").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithEmptyList(): Unit ={
    val res = runOnDemoGraph(
      """
        |UNWIND [] AS empty
        |RETURN empty, 'literal_that_is_not_returned'
        |""".stripMargin).records().toArray
    Assert.assertEquals(0, res.length)
  }

  @Test
  def usingUNWINDWithAnExpressionThatIsNotAList(): Unit ={
    val res = runOnDemoGraph(
      """
        |UNWIND [] AS empty
        |RETURN empty, 'literal_that_is_not_returned'
        |""".stripMargin).records().toArray
    Assert.assertEquals(0, res.length)
  }
}
