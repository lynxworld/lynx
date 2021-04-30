package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherDataTypeTest extends TestBase {

  @Test
  def testLynxInteger(): Unit = {
    val r1 = runOnDemoGraph("RETURN 98988928384899 AS numLong, 938439 as numInt").records().next()
    Assert.assertEquals(98988928384899L, r1.get("numLong").get.asInstanceOf[LynxInteger].value)
    Assert.assertEquals(938439L, r1.get("numInt").get.asInstanceOf[LynxInteger].value)
  }

}
