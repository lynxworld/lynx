package org.grapheco.virtual

import org.grapheco.lynx.TestBase
import org.junit.jupiter.api.Test

class VirtualTest extends TestBase{

  @Test
  def test(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n)~[:contains]~~<m>
        |RETURN n,m
        |""".stripMargin)
  }
}
