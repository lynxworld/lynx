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
    var rs = runOnDemoGraph(q).records().toSeq
    println(rs)
  }
}
