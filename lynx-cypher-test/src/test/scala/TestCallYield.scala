import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class TestCallYield {
  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def callProcedureUsingCall(): Unit ={
    testBase.runOnDemoGraph(
      """
        |CALL `db`.`labels`
        |""".stripMargin)
  }
}
