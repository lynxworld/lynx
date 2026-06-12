package org.grapheco.LDBC

import org.grapheco.LDBC.LDBCCyphers._
import org.grapheco.lynx.TestBase
import org.grapheco.lynx.parser.DefaultQueryParser
import org.junit.jupiter.api.Test


@Test
class LDBCTest extends TestBase {

  val time = 10
  val all: List[String] = List(IS1, IS2, IS3, IS4, IS5, IS6, IS7, IU1, IU2, IU3, IU4, IU5, IU6, IU7, IU8)


  @Test
  def testldbc(): Unit = {

    val paras = LDBCCyphers.parameters
    all.foreach(p => runner.run(p,paras))
    all.foreach(p => runner.run(p,paras).show())

  }

  @Test
  def parseTest(): Unit = {
    val parser = new DefaultQueryParser(runner.runnerContext)
    val times = 1000
//    Thread.sleep(2000)
    val result = all.map { p =>
      val t0 = System.currentTimeMillis()
      for (i <- 0 until times) {
        parser.parse(p)._2.toArray
      }
      val t = System.currentTimeMillis() - t0
      t
    }
    println(result.mkString(","))
  }

  @Test
  def test(): Unit = {
    runner.run(
      """
        |return <http://aaa.jpg>->
        |""".stripMargin, Map.empty)
  }

  @Test
  def test2(): Unit = {
    runner.run(
      """
        |MATCH (n) RETURN COUNT(n)
        |""".stripMargin, Map.empty).show()
  }

}
