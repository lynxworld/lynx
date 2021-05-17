package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherDeleteTest extends TestBase {
  @Test
  def testDeleteNode(): Unit = {
    var rs = runOnDemoGraph("MATCH (n) Delete n")
  }
}
