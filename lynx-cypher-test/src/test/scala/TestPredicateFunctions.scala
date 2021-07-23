import org.grapheco.lynx.{LynxInteger, LynxList, LynxString}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-21 14:53
 */
class TestPredicateFunctions {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("Alice")), ("age", LynxInteger(38)), ("eyes", LynxString("brown")))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("Bob")), ("age", LynxInteger(25)), ("eyes", LynxString("blue")))
  val n3 = TestNode(3, Seq.empty, ("name",LynxString("Charlie")), ("age", LynxInteger(53)), ("eyes", LynxString("green")))
  val n4 = TestNode(4, Seq.empty, ("name",LynxString("Daniel")), ("age", LynxInteger(54)), ("eyes", LynxString("brown")), ("like_colors", LynxList(List.empty)))
  val n5 = TestNode(5, Seq.empty, ("name",LynxString("Eskil")), ("age", LynxInteger(41)), ("eyes", LynxString("blue")),
    ("like_colors", LynxList(List(LynxString("pink"), LynxString("yellow"), LynxString("black")))))
  val n6 = TestNode(5, Seq.empty, ("alias",LynxString("Frank")), ("age", LynxInteger(61)), ("eyes", LynxString("")),
    ("like_colors", LynxList(List(LynxString("blue"), LynxString("green")))))

  val r1 = TestRelationship(1, 1, 2, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 2, 4, Option("KNOWS"))
  val r4 = TestRelationship(4, 2, 5, Option("MARRIED"))
  val r5 = TestRelationship(5, 3, 4, Option("KNOWS"))

  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, n6)
  relsBuffer.append(r1, r2, r3, r4, r5)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def exists(): Unit ={
    testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE exists((n)-[:MARRIED]->()-[:MARRIED]->())
        |RETURN n
        |""".stripMargin)
  }
}
