import org.grapheco.lynx.LynxString
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-07 10:49
 */
class TestUnion {
  val n1 = TestNode(1, Seq("Actor"), ("name",LynxString("Anthony Hopkins")))
  val n2 = TestNode(2, Seq("Actor"), ("name",LynxString("Hitchcock")))
  val n3 = TestNode(3, Seq("Actor"), ("name",LynxString("Hellen Mirren")))
  val n4 = TestNode(4, Seq("Movie"), ("name",LynxString("Hitchcock")))

  val r1 = TestRelationship(1, 1, 3, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 4, Option("ACTS_IN"))
  val r3 = TestRelationship(3, 3, 4, Option("ACTS_IN"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4)
  relsBuffer.append(r1, r2, r3)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def combineTwoQueriesAndRetainDuplicates(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION ALL
        |MATCH (n:Movie)
        |RETURN n.title AS name
        |""".stripMargin)
  }
}
