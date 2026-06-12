package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 16:41
 */
class C_Return extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("A"), LynxPropertyKey("age")-> LynxValue(55), LynxPropertyKey("happy")-> LynxValue("YES!")))
  val n2 = TestNode(TestId(2), Seq.empty, Map(LynxPropertyKey("name")-> LynxValue("B")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("BLOCKS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)


  @BeforeEach
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }
  @Test
  def returnNodes(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n {name: 'B'})
        |RETURN n
        |""".stripMargin).records().toArray

    Assertions.assertEquals(n2, res(0)("n").asInstanceOf[LynxNode])
  }

  @Test
  def returnRelationships(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n {name: 'A'})-[r:KNOWS]->(c)
        |RETURN r
        |""".stripMargin).records().toArray

    Assertions.assertEquals(r2, res(0)("r").asInstanceOf[LynxRelationship])
  }

  @Test
  def returnProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n {name: 'A'})
        |RETURN n.name
        |""".stripMargin).records().toArray

    Assertions.assertEquals(n1.property(LynxPropertyKey("name")).get, res(0)("n.name").asInstanceOf[LynxValue])
  }

  @Test
  def returnAllElement(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH p = (a {name: 'A'})-[r]->(b)
        |RETURN *
        |""".stripMargin).records().toArray

    Assertions.assertEquals(n1, res(0)("a").asInstanceOf[LynxNode])
    Assertions.assertEquals(n2, res(0)("b").asInstanceOf[LynxNode])
    Assertions.assertEquals(r1, res(0)("r").asInstanceOf[LynxRelationship])
    Assertions.assertEquals(r2, res(1)("r").asInstanceOf[LynxRelationship])
    Assertions.assertEquals(List(n1, r1, n2), res(0)("p").asInstanceOf[LynxValue].value)
    Assertions.assertEquals(List(n1, r2, n2), res(1)("p").asInstanceOf[LynxValue].value)
  }

  @Test
  def variableWithUncommonCharacters(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (`This isn\'t a common variable`)
        |WHERE `This isn\'t a common variable`.name = 'A'
        |RETURN `This isn\'t a common variable`.happy
        |""".stripMargin).records().toArray

    Assertions.assertEquals("YES!", res(0)("`This isn\\'t a common variable`.happy").asInstanceOf[LynxValue].value)
  }

  @Test
  def columnAlias(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})
        |RETURN a.age AS SomethingTotallyDifferent
        |""".stripMargin).records().toArray

    Assertions.assertEquals(55L, res(0)("SomethingTotallyDifferent").asInstanceOf[LynxValue].value)
  }

  @Test
  def optionalProperties(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.age
        |""".stripMargin).records().toArray

    Assertions.assertEquals(55L, res(0)("n.age").asInstanceOf[LynxValue].value)
    Assertions.assertEquals(null, res(1)("n.age").asInstanceOf[LynxValue].value)
  }

  @Test
  def uniqueResults(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})-->(b)
        |RETURN DISTINCT b
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, res.length)
    Assertions.assertEquals(n2, res(0)("b").asInstanceOf[LynxNode])
  }

  @Test
  def directlyReturn(): Unit ={
    val res = runOnDemoGraph(
      """
        |RETURN 1
        |""".stripMargin).records().toArray
    Assertions.assertEquals(1, res.length)
  }

  @Test
  def otherExpressions(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a {name: 'A'})
        |RETURN a.age > 30, "I'm a literal", (a)-->()
        |""".stripMargin).records().toArray
  }
}
