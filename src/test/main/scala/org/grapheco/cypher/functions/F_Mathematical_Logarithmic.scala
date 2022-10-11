package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-01 09:30
 */
class F_Mathematical_Logarithmic extends TestBase {

  @Test
  def e(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN e()
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(2.718281828459045, records(0)("e()").asInstanceOf[LynxValue].value)
  }

  @Test
  def exp(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN exp(2)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(7.38905609893065, records(0)("exp(2)").asInstanceOf[LynxValue].value)
  }

  @Test
  def log(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN log(27)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3.295836866004329, records(0)("log(27)").asInstanceOf[LynxValue].value)
  }

  @Test
  def log10(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN log10(27)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.4313637641589874, records(0)("log10(27)").asInstanceOf[LynxValue].value)
  }

  /*
  return should be a float.
   */
  @Test
  def sqrt(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN sqrt(256)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(16.0, records(0)("sqrt(256)").asInstanceOf[LynxValue].value)
  }


}




