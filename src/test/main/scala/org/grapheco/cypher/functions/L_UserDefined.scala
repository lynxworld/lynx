package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-01 16:07
 */
class L_UserDefined extends TestBase {

  @Test
  def callUserDefinedFunction(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Member)
        |RETURN test.authors() AS members
        |""".stripMargin).records().toArray

//    Assert.assertEquals(1, records.length)
//    Assert.assertEquals("John,Paul,George,Ringo", records(0)("members").asInstanceOf[LynxValue].value)
  }

  @Test
  def callUserDefinedAggregationFunction(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Member)
        |RETURN toInterger("2023") AS member
        |""".stripMargin).records().toArray

//    Assert.assertEquals(1, records.length)
//    Assert.assertEquals("George", records(0)("member").asInstanceOf[LynxValue].value)
  }


}




