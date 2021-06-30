package org.grapheco.lynx

import org.junit.{Assert, Test}
import org.junit.function.ThrowingRunnable

import scala.collection.mutable.ArrayBuffer

class CypherDeleteTest extends TestBase {

  val n1 = TestNode(1, Array("Person"), ("name",LynxString("Andy")), ("age", LynxInteger(36)))
  val n2 = TestNode(2, Array("Person"), ("name",LynxString("UNKNOWN")))
  val n3 = TestNode(3, Array("Person"), ("name",LynxString("Timothy")), ("age", LynxInteger(25)))
  val n4 = TestNode(4, Array("Person"), ("name",LynxString("Peter")), ("age", LynxInteger(34)))

  val r1 = TestRelationship(1, 1, 3, Option("KNOWS"))
  val r2 = TestRelationship(2, 1, 4, Option("KNOWS"))

  all_nodes.clear()
  all_nodes.append(n1, n2, n3, n4)
  all_rels.clear()
  all_rels.append(r1, r2)

  @Test
  def testDeleteNode(): Unit = {
    Assert.assertThrows(classOf[ConstrainViolatedException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("MATCH (n) Delete n")
      }
    })
  }
  @Test
  def testDeleteDetachNode(): Unit = {
    var res = runOnDemoGraph("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(4L, res)
    runOnDemoGraph("MATCH (n) Detach Delete n")
    res = runOnDemoGraph("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(0L, res)
  }

  @Test
  def deleteSingleNode(): Unit ={
    val num = all_nodes.length
    runOnDemoGraph(
      """
        |MATCH (n:Person {name: 'UNKNOWN'})
        |DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, all_nodes.length)
  }

  @Test
  def deleteAllNodesAndRelationships(): Unit ={
    runOnDemoGraph(
      """
        |MATCH (n)
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(0, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def deleteANodeWithAllItsRelationships(): Unit ={
    val num = all_nodes.length

    runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})
        |DETACH DELETE n
        |""".stripMargin)

    Assert.assertEquals(num - 1, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def deleteRelationshipsOnly(): Unit ={
    val num = all_nodes.length

    runOnDemoGraph(
      """
        |MATCH (n {name: 'Andy'})-[r:KNOWS]->()
        |DELETE r
        |""".stripMargin)

    Assert.assertEquals(num, all_nodes.size)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def deleteNodesAndRelationships(): Unit ={
    val nodeNum = all_nodes.length
    val relNum = all_rels.length

    runOnDemoGraph(
      """
        |MATCH ({name: 'Andy'})-[r:KNOWS]->(n {name: 'Timothy'})
        |DETACH DELETE n, r
        |""".stripMargin)

    Assert.assertEquals(nodeNum - 1, all_nodes.size)
    Assert.assertEquals(relNum - 1, all_rels.size)
  }
}
