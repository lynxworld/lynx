package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherJoinTest extends TestBase {
  @Test
  def testMatchAndCreateNode(): Unit = {
    val q = """MATCH
    (a:person{name:'Alice'}),
    (b:person{name:'bluejoe'}),
    (c) where c.name = 'Bob'
    CREATE (a)-[r]->(b)
    RETURN a,b,c, r""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("a").get.asInstanceOf[LynxNode]
    val b = rs.get("b").get.asInstanceOf[LynxNode]
    val c = rs.get("c").get.asInstanceOf[LynxNode]
    val r = rs.get("r").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals("Alice", a.property("name").get.value)
    Assert.assertEquals("bluejoe", b.property("name").get.value)
    Assert.assertEquals("Bob", c.property("name").get.value)
    Assert.assertEquals(a.id, r.startNodeId)
    Assert.assertEquals(b.id, r.endNodeId)
  }
}
