package org.grapheco.lynx

import org.junit.{Assert, Test}
import org.junit.function.ThrowingRunnable

class CypherJoinTest extends TestBase {
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
    Assert.assertEquals(3L, res)
    runOnDemoGraph("MATCH (n) Detach Delete n")
    res = runOnDemoGraph("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    Assert.assertEquals(0L, res)
  }
}
