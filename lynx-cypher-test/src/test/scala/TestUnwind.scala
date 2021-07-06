import org.grapheco.lynx.LynxString
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: not impl
 * @author: LiamGao
 * @create: 2021-07-05 13:36
 */
class TestUnwind {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("Anders")))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("Caesar")))
  val n3 = TestNode(3, Seq.empty, ("name",LynxString("Bossman")))
  val n4 = TestNode(4, Seq.empty, ("name",LynxString("George")))
  val n5 = TestNode(5, Seq.empty, ("name",LynxString("David")))


  val r1 = TestRelationship(1, 1, 2, Option("BLOCKS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 2, 4, Option("KNOWS"))
  val r4 = TestRelationship(4, 3, 4, Option("KNOWS"))
  val r5 = TestRelationship(5, 3, 5, Option("BLOCKS"))
  val r6 = TestRelationship(6, 5, 1, Option("KNOWS"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5)
  relsBuffer.append(r1, r2, r3, r4, r5, r6)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def creatingADistinctList(): Unit ={
    testBase.runOnDemoGraph(
      """
        |UNWIND [1, 2, 3, null] AS x
        |RETURN x, 'val' AS y
        |""".stripMargin)
  }
}
