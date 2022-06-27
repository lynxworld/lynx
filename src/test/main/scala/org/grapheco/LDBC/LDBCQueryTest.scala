package org.grapheco.LDBC


import org.grapheco.lynx.{LynxResult, TestBase}
import org.junit.Test

import java.io.File
import scala.io.Source

/**
 * @ClassName LDBCShortQueryTest
 * @Description
 * @Author Hu Chuan
 * @Date 2022/6/27
 * @Version 0.1
 */
class LDBCQueryTest extends TestBase{
  def getQuery(name: String): String = {
    val path = this.getClass.getResource("/LDBC")
    val file = new File(path.getPath + "/" + name)
    val s = Source.fromFile(file)
    val query = s.mkString
    s.close()
    query
  }

  def run(cypher: String, params: Map[String, Any]): LynxResult ={
    val r = runOnDemoGraph(cypher, params)
    r.show()
    r
  }

  @Test
  def IS1(): Unit ={
    val q = getQuery("interactive-short-1.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS2(): Unit ={
    val q = getQuery("interactive-short-2.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS3(): Unit ={
    val q = getQuery("interactive-short-3.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS4(): Unit ={
    val q = getQuery("interactive-short-4.cypher")
    val p = Map("messageId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS5(): Unit ={
    val q = getQuery("interactive-short-5.cypher")
    val p = Map("messageId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS6(): Unit ={
    val q = getQuery("interactive-short-6.cypher")
    val p = Map("messageId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def IS7(): Unit ={
    val q = getQuery("interactive-short-7.cypher")
    val p = Map("messageId" -> "10995116277794")
    run(q,p)
  }

}
