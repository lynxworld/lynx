import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-07 10:55
 */
class TestCall {
  val n1 = TestNode(1, Seq("Person", "Child"), ("name",LynxString("Alice")), ("age", LynxInteger(20)))
  val n2 = TestNode(2, Seq("Person"),  ("name",LynxString("Dora")), ("age", LynxInteger(30)))
  val n3 = TestNode(3, Seq("Person", "Parent"), ("name",LynxString("Charlie")), ("age", LynxInteger(65)))
  val n4 = TestNode(4, Seq("Person"), ("name",LynxString("Bob")), ("age", LynxInteger(27)))

  val r1 = TestRelationship(1, 1, 3, Option("CHILD_OF"))
  val r2 = TestRelationship(2, 1, 4, Option("FRIEND_OF"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4)
  relsBuffer.append(r1, r2)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def importingVariablesIntoSubqueries(): Unit ={
    val res = testBase.runOnDemoGraph(
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
    val res = testBase.runOnDemoGraph(
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
