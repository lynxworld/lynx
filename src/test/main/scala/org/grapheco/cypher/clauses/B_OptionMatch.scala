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
 * @create: 2022-02-28 15:42
 */
class B_OptionMatch extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Charlie Sheen")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Oliver Stone")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Michael Douglas")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Martin Sheen")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("person")), Map(LynxPropertyKey("name")-> LynxValue("Rob Reiner")))
  val m1 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("name")-> LynxValue("Wall Street")))
  val m2 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("name")-> LynxValue("The American President"))


  val r1 = TestRelationship(TestId(1), TestId(1), TestId(4), Option(LynxRelationshipType("FATHER")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(6), Option(LynxRelationshipType("DIRECTED")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r6 = TestRelationship(TestId(6), TestId(4), TestId(6), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r7 = TestRelationship(TestId(7), TestId(4), TestId(7), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  val r8 = TestRelationship(TestId(8), TestId(5), TestId(7), Option(LynxRelationshipType("DIRECTED")), Map.empty)


  @Before
  def init(): Unit ={
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
  }
  @Test
  def optionalRelationships(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-->(x)
        |RETURN x
        |""".stripMargin).records().toArray
    Assert.assertEquals(null, res(0)("x").asInstanceOf[LynxValue].value)
  }

  @Test
  def propertiesOnOptionalElements(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-->(x)
        |RETURN x, x.name
        |""".stripMargin).records().toArray
    Assert.assertEquals(null, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(0)("x.name").asInstanceOf[LynxValue].value)
  }

  @Test
  def optionalTypedAndNamedRelationship(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a:Movie { title: 'Wall Street' })
        |OPTIONAL MATCH (a)-[r:ACTS_IN]->()
        |RETURN a.title, r
        |""".stripMargin).records().toArray
    Assert.assertEquals("Wall Street", res(0)("a.title").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(0)("r").asInstanceOf[LynxValue].value)
  }
}
