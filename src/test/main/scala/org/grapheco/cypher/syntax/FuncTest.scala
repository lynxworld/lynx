package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.junit.Test

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/9/15
 * @version: 1.0
 */
class FuncTest extends TestBase{

  @Test
  def testPercentile():Unit={
    val records = runOnDemoGraph("RETURN percentileCont([37,12 ,72 ,9 ,75 ,5, 79, 64 ,16 ,1 ,76, 71, 6 ,25, 50 ,20 ,18 ,84 ,11 ,28], 0.75)")
  }
}
