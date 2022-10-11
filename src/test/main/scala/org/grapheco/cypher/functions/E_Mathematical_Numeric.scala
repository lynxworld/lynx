package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-08-30 14:15
 */
class E_Mathematical_Numeric extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("A")),
    Map(LynxPropertyKey("name") -> LynxValue("Alice"),
      LynxPropertyKey("age") -> LynxValue("38"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("C")),
    Map(LynxPropertyKey("name") -> LynxValue("Charlie"),
      LynxPropertyKey("age") -> LynxValue("53"),
      LynxPropertyKey("eyes") -> LynxValue("green")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("B")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"),
      LynxPropertyKey("age") -> LynxValue("25"),
      LynxPropertyKey("eyes") -> LynxValue("blue")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("D")),
    Map(LynxPropertyKey("name") -> LynxValue("Daniel"),
      LynxPropertyKey("age") -> LynxValue("54"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("E")),
    Map(LynxPropertyKey("name") -> LynxValue("Eskil"),
      LynxPropertyKey("age") -> LynxValue("38"),
      LynxPropertyKey("eyes") -> LynxValue("brown"),
      LynxPropertyKey("array") -> LynxValue(Array("one", "two", "three"))))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("MARRIED")), Map.empty)

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), Seq.empty, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), Seq.empty, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), Seq.empty, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), Seq.empty, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  /*
  no value returned
   */
  @Test
  def abs(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (a),(e)
        |WHERE a.name = 'Alice' AND e.name = 'Eskil'
        |RETURN a.age, e.age, abs(a.age - e.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(38, records(0)("a.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(41, records(0)("e.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, records(0)("abs(a.age - e.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def ceil(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN ceil(0.1)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.0, records(0)("ceil(0.1)").asInstanceOf[LynxValue].value)
  }

  @Test
  def floor(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN floor(0.9)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.0, records(0)("floor(0.9)").asInstanceOf[LynxValue].value)
  }

  @Test
  def rand(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN rand()
        |""".stripMargin).records().toArray

    val rand = records(0)("rand()").asInstanceOf[LynxValue].value.asInstanceOf[Double]
    val flag = (0 <= rand) && (1 > rand)

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(true, flag)
  }

  /*
    return should be a float, expected 3.0 in neo4j Docs but return 3
     */
  @Test
  def round(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN round(3.141592)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3l, records(0)("round(3.141592)").asInstanceOf[LynxValue].value)
  }

  /*
  return should be an Integer
   */
  @Test
  def sign(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN sign(-17), sign(0.1)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(-1.0, records(0)("sign(-17)").asInstanceOf[LynxValue].value)
    Assert.assertEquals(1.0, records(0)("sign(0.1)").asInstanceOf[LynxValue].value)
  }
}



