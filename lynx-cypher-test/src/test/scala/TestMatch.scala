import org.grapheco.lynx.LynxString
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:53 上午 2021/6/17
 * @Modified By:
 */
class TestMatch {
  val node1 = new TestNode(1, Array("Person"), ("name",LynxString("Oliver Stone")))
  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(node1)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def getAllNodes(): Unit ={
    val records = testBase.runOnDemoGraph("Match(n) Return n;")
    records.records().foreach(println)
  }


}
