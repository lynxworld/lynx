package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherJoinTest extends TestBase {

  @Test
  def testMany2ManySingleMatch(): Unit = {
    val q = """MATCH
    (a:person),
    (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assert.assertEquals(2, a.value)
  }

  @Test
  def testMany2ManyMultiMatch(): Unit = {
    val q = """MATCH
    (a:person)
    MATCH (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assert.assertEquals(4, a.value)
  }
}
