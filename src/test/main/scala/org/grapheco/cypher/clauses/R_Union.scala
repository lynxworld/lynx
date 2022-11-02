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
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Actor")), Map(LynxPropertyKey("name")-> LynxValue("Hellen Mirren"), LynxPropertyKey("age")-> LynxValue(36), LynxPropertyKey("hungry")->LynxValue(true)))
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


    nodesInput.foreach(e=>{
      print(s"create (a:${e._2.labels.toList(0).value}{")
      val props = e._2.props.toList
      for(i <-props.indices){
          if(i == props.size-1){
            print(s"'${props(i)._1.value}':'${props(i)._2.value}'")
          }else{
            print(s"'${props(i)._1.value}':'${props(i)._2.value}',")
          }
      }
      println("})")
    })

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))



    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def combineTwoQueriesAndRetainDuplicates(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION ALL
        |MATCH (n:Movie)
        |RETURN n.title AS name
        |""".stripMargin).records().toArray

    Assert.assertEquals("Anthony Hopkins", res(0)("name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Helen Mirren", res(1)("name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Hitchcock", res(2)("name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Hitchcock", res(3)("name").asInstanceOf[LynxValue].value)

  }

  @Test
  def combineTwoQueriesAndRemoveDuplicates(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION
        |MATCH (n:Movie)
        |RETURN n.title AS name
        |""".stripMargin).records().toArray

    Assert.assertEquals("Anthony Hopkins", res(0)("name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Helen Mirren", res(1)("name").asInstanceOf[LynxValue].value)
    Assert.assertEquals("Hitchcock", res(2)("name").asInstanceOf[LynxValue].value)
  }
}
