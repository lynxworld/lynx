import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-18 15:43
 */
class TestJoin {
  val n1 = TestNode(1, Array("Person"), ("name",LynxString("Charlie Sheen")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("John Brown")), ("age",LynxInteger(1)))
  val n2 = TestNode(2, Array("Person"), ("name",LynxString("Oliver Stone")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("Bill White")), ("age",LynxInteger(2)))
  val n3 = TestNode(3, Array("Person"), ("name",LynxString("Michael Douglas")), ("bornIn", LynxString("New Jersey")), ("chauffeurName", LynxString("John Brown")))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Martin Sheen")), ("bornIn", LynxString("Ohio")), ("chauffeurName", LynxString("Bob Brown")))
  val n5 = TestNode(5, Array("Person"), ("name",LynxString("Rob Reiner")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("Ted Green")))
  val m1 = TestNode(6, Array("Movie"), ("title",LynxString("Wall Street")), ("bornIn", LynxString("New York")), ("chauffeurName", LynxString("John Brown")))
  val m2 = TestNode(7, Array("Movie"), ("title",LynxString("The American President")))

  val r1 = TestRelationship(1, 1, 6, Option("ACTED_IN"))
  val r2 = TestRelationship(2, 1, 4, Option("FATHER"))
  val r3 = TestRelationship(3, 2, 6, Option("ACTED_IN"))
  val r4 = TestRelationship(4, 3, 6, Option("ACTED_IN"))
  val r5 = TestRelationship(5, 3, 7, Option("ACTED_IN"))
  val r6 = TestRelationship(6, 4, 6, Option("ACTED_IN"))
  val r7 = TestRelationship(7, 4, 7, Option("ACTED_IN"))
  val r8 = TestRelationship(8, 5, 7, Option("ACTED_IN"))



  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, m1, m2)
  relsBuffer.append(r1, r2, r3, r4, r5, r6, r7, r8)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def test(): Unit ={
    testBase.runOnDemoGraph(
      """
        |match (n:Person{name:'Charlie Sheen'})
        |match (m:Person{bornIn: n.bornIn})
        |return m
        |""".stripMargin)
  }
}
