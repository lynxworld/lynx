package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxNull, LynxString}
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 14:11 2022/6/27
 * @Modified By:
 */
class ScalarFunctionsTest extends TestBase {

  @Test
  def testCoalesce(): Unit = {
    runOnDemoGraph("Create(n{name:'Alice', age:38, eyes:'brown'})").show()
//    runOnDemoGraph("MATCH (a) WHERE a.name = 'Alice' Return a.hairColor").show()
    runOnDemoGraph("MATCH (a) WHERE a.name = 'Alice' RETURN a").show()
    val brown1 = runOnDemoGraph("MATCH (a) WHERE a.name = 'Alice' RETURN coalesce(a.hair, a.eyes) as result;")
      .records().next().get("result").get.asInstanceOf[LynxString].value
    Assert.assertEquals("brown", brown1)

    val brown2 = runOnDemoGraph("MATCH (a) WHERE a.name = 'Alice' RETURN coalesce(a.eyes, a.hair) as result;")
      .records().next().get("result").get.asInstanceOf[LynxString].value
    Assert.assertEquals("brown", brown2)

    val nullResult: LynxValue = runOnDemoGraph("MATCH (a) WHERE a.name = 'Alice' RETURN coalesce(a.hair) as result;")
      .records().next().get("result").get
    Assert.assertTrue(nullResult.isInstanceOf[LynxNull.type])
  }

}
