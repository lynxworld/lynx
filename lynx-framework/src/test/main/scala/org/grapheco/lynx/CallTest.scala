package org.grapheco.lynx

import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CallTest extends TestBase {
  @Test
  def testNonArgsCall(): Unit = {
    var rs: LynxResult = null
    rs = runOnDemoGraph("call test.authors()")
    Assert.assertEquals(Seq("name"), rs.columns)
    Assert.assertEquals(3, rs.records().size)
    Assert.assertEquals(Map("name" -> LynxValue("bluejoe")), rs.records().toSeq.apply(0))
    Assert.assertEquals(Map("name" -> LynxValue("lzx")), rs.records().toSeq.apply(1))
    Assert.assertEquals(Map("name" -> LynxValue("airzihao")), rs.records().toSeq.apply(2))
  }

  @Test
  def testWrongCall(): Unit = {
    Assert.assertThrows(classOf[UnknownProcedureException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.nonexisting()")
      }
    })

    Assert.assertThrows(classOf[WrongNumberOfArgumentsException], new ThrowingRunnable() {
      override def run(): Unit = {
        runOnDemoGraph("call test.authors(2)")
      }
    })
  }
  @Test
  def testSumSimple(): Unit = {
    val rs = runOnDemoGraph("match (n) return sum(n.age)").records().next()
  }
}
