package org.grapheco.lynx

import org.grapheco.lynx.types.property.LynxInteger
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}
class CypherJoinTest extends TestBase {

  runOnDemoGraph(
    """
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c{name:"Bob", age: 10, gender:"male"}),
      |(d{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)

  @Test
  def testMany2ManySingleMatch(): Unit = {
    val q = """MATCH
    (a:person),
    (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assertions.assertEquals(4, a.value)
  }

  @Test
  def testMany2ManyMultiMatch(): Unit = {
    val q = """MATCH
    (a:person)
    MATCH (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assertions.assertEquals(4, a.value)
  }
}
