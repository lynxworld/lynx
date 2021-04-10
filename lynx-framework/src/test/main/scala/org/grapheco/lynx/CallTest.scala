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
  def testCount(): Unit = {
    val rs = runOnDemoGraph("match (n) return  count(n.name),count(n.age),count(n.nonexist),count(1),count(1+1)").records().next()
    Assert.assertEquals(LynxValue(3), rs("count(n.name)"))
    Assert.assertEquals(LynxValue(3), rs("count(n.age)"))
    Assert.assertEquals(LynxValue(0), rs("count(n.nonexist)"))
    Assert.assertEquals(LynxValue(3), rs("count(1)"))
    Assert.assertEquals(LynxValue(3), rs("count(1+1)"))
  }

  @Test
  def testSum(): Unit = {
    runOnDemoGraph("match (n) return  sum(n.name)")
    var rs = runOnDemoGraph("match (n) return  sum(n.age),sum(n.nonexist),sum(1),sum(1+1)").records().next()
  }
}
