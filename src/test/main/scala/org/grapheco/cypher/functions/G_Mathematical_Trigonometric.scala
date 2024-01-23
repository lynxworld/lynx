package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.NodeInput
import org.grapheco.lynx.types.LynxValue
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-01 10:02
 */
class G_Mathematical_Trigonometric extends TestBase {

  @Test
  def acos(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN acos(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.0471975511965979, records(0)("acos(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def asin(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN asin(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.5235987755982989, records(0)("asin(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def atan(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN atan(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.4636476090008061, records(0)("atan(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def atan2(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN atan2(0.5, 0.6)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.6947382761967033, records(0)("atan2(0.5, 0.6)").asInstanceOf[LynxValue].value)
  }


  @Test
  def cos(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN cos(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.8775825618903728, records(0)("cos(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def cot(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN cot(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.830487721712452, records(0)("cot(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def degrees(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN degrees(3.14159)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(179.9998479605043, records(0).getAsDouble("degrees(3.14159)").get.v, 0.000001)
  }

  @Test
  def haversin(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN haversin(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.06120871905481362, records(0)("haversin(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def sphericalDistanceUsingTheHaversin(): Unit = {
    val nodesInput = ArrayBuffer[(String, NodeInput)]()
    val node_num = nodesInput.length
    val records = runOnDemoGraph(
      """
        |CREATE (ber:City { lat: 52.5, lon: 13.4 }),(sm:City { lat: 37.5, lon: -122.3 })
        |RETURN 2 * 6371 * asin(sqrt(haversin(radians(sm.lat - ber.lat))+ cos(radians(sm.lat))* cos(radians(ber.lat))* haversin(radians(sm.lon - ber.lon)))) AS dist
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(9129.969740051658, records(0)("dist").asInstanceOf[LynxValue].value)
    Assert.assertEquals(node_num + 2, all_nodes.size)
    //TODO properties and labels
  }

  @Test
  def pi(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN pi()
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3.141592653589793, records(0)("pi()").asInstanceOf[LynxValue].value)
  }

  @Test
  def radians(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN radians(180)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3.141592653589793, records(0)("radians(180)").asInstanceOf[LynxValue].value)
  }

  @Test
  def sin(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN sin(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.479425538604203, records(0)("sin(0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def tan(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN tan(0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(0.5463024898437905, records(0)("tan(0.5)").asInstanceOf[LynxValue].value)
  }
}




