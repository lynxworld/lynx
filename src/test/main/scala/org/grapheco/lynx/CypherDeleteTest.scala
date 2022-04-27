package org.grapheco.lynx
import org.grapheco.lynx.runner.ConstrainViolatedException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.NameParser._
import org.junit.{Assert, Test}
import org.junit.function.ThrowingRunnable


class CypherDeleteTest extends TestBase {

  runOnDemoGraph(
    """
      |Create
      |(a:Person{name:"Andy", age: 36}),
      |(b:Person{name:"UNKNOWN"}),
      |(c:Person{name:"Timothy", age: 25}),
      |(d:Person{name:"Peter", age: 34}),
      |(a)-[:KNOWS]->(c),
      |(a)-[:KNOWS]->(d)
      |""".stripMargin)

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
