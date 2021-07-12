import org.grapheco.lynx.LynxString
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class TestForeach {
  val n3 = TestNode(1, Array("Person"), ("name",LynxString("A")))
  val n1 = TestNode(2, Array("Person"), ("name",LynxString("B")))
  val n2 = TestNode(3, Array("Person"), ("name",LynxString("C")))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("D")))

  val r1 = TestRelationship(1, 1, 2, Option("KNOWS"))
  val r2 = TestRelationship(2, 2, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 3, 4, Option("KNOWS"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4)
  relsBuffer.append(r1, r2, r3)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def markAllNodesAloneAPath(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH p =(begin)-[*]->(END )
        |WHERE begin.name = 'A' AND END .name = 'D'
        |FOREACH (n IN nodes(p)| SET n.marked = TRUE )
        |""".stripMargin)
  }
}
