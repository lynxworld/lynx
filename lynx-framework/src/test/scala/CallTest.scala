import org.grapheco.lynx.{LynxResult, LynxValue}
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
}
