package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherDeleteTest extends TestBase {
  @Test
  def testDeleteNode(): Unit = {
    var rs = runOnDemoGraph("MATCH (n) Delete n")
  }
  @Test
  def testDeleteDetachNode(): Unit = {
    var rs = runOnDemoGraph("MATCH (n) DETACH DELETE n")
    rs = runOnDemoGraph("MATCH (n) return count(n)")
  }
}
