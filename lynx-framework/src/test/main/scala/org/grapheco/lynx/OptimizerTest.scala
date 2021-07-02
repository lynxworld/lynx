package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-06-30 14:08
 */
class OptimizerTest extends TestBase {

  @Test
  def testEstimate(): Unit ={
    val res = runOnDemoGraph2(
      """
        |match (n:person)
        |match (m:leader)
        |return n,m
        |""".stripMargin).asInstanceOf[PlanAware]

   Assert.assertEquals("PPTJoin(None,0)", res.getOptimizerPlan().children.head.toString)
  }
}
