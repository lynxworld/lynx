package org.grapheco.cypher

import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.{NodeInput, RelationshipInput, StoredNodeInputRef, TestBase, types}
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:25
 */
class M_Remove extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Swedish")), Map(LynxPropertyKey("name")-> LynxValue("Andy"), LynxPropertyKey("age")-> LynxValue(36)))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Swedish"), LynxNodeLabel("German")), Map(LynxPropertyKey("name")-> LynxValue("Peter"), LynxPropertyKey("age")-> LynxValue(34)))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Swedish")), Map(LynxPropertyKey("name")-> LynxValue("Timothy"), LynxPropertyKey("age")-> LynxValue(25)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
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
  def removeAProperty(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (a {name: 'Andy'})
        |REMOVE a.age
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Andy", records.head("a.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(LynxNull, records.head("a.age").asInstanceOf[LynxValue])
  }

  @Test
  def removeALabelFromANode(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Peter'})
        |REMOVE n:German
        |RETURN n.name, labels(n)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List("Swedish"), records.head("labels(n)").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }

  @Test
  def removeMultipleLabelsFromANode(): Unit ={
    val records = runOnDemoGraph(
      """
        |MATCH (n {name: 'Peter'})
        |REMOVE n:German:Swedish
        |RETURN n.name, labels(n)
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Peter", records.head("n.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List.empty, records.head("labels(n)").asInstanceOf[LynxValue].value.asInstanceOf[List[LynxValue]].map(f => f.value))
  }

}
