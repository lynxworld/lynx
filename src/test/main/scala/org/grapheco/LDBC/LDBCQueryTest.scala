package org.grapheco.LDBC

import org.grapheco.LDBC.LDBCQueryTest.ldbcTestBase
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.util.Profiler
import org.junit.{Assert, BeforeClass, Test}

import java.io.File
import scala.io.Source

/**
 * @ClassName LDBCShortQueryTest
 * @Description
 * @Author Hu Chuan
 * @Date 2022/6/27
 * @Version 0.1
 */

object LDBCQueryTest {
  val path = this.getClass.getResource("/LDBC/LDBC0.003").getPath
  var personIds: Array[LynxInteger] = _
  var commontIds: Array[LynxInteger] = _

  val ldbcTestBase: LDBCTestBase = new LDBCTestBase

  @BeforeClass
  def importData(): Unit ={
    Profiler.timing("Import the test data. ", ldbcTestBase.loadLDBC(path))
  }
}

class LDBCQueryTest {

  def getQuery(name: String): String = {
    val path = this.getClass.getResource("/LDBC")
    val file = new File(path.getPath + "/" + name)
    val s = Source.fromFile(file)
    val query = s.mkString
    s.close()
    query
  }

  def run(cypher: String, params: Map[String, Any]): Unit ={
    try {
      val r = ldbcTestBase.run(cypher, params)
    } catch {
      case ex: Exception => Assert.assertEquals("ShortestPaths not supported.", ex.getMessage)
      case _ => Assert.assertTrue(false)
    }
  }

  @Test
  def IS1(): Unit ={
    val q = getQuery("interactive-short-1.cypher")
    val p = Map("personId" -> "210995116277782")
    run(q,p)
  }

  @Test
  def IS2(): Unit ={
    val q = getQuery("interactive-short-2.cypher")
    val p = Map("personId" -> "210995116277782")
    run(q,p)
  }

  @Test
  def IS3(): Unit ={
    val q = getQuery("interactive-short-3.cypher")
    val p = Map("personId" -> "210995116277782")
    run(q,p)
  }

  @Test
  def IS4(): Unit ={
    val q = getQuery("interactive-short-4.cypher")
    val p = Map("messageId" -> "101030792151058")
    run(q,p)
  }

  @Test
  def IS5(): Unit ={
    val q = getQuery("interactive-short-5.cypher")
    val p = Map("messageId" -> "101030792151058")
    run(q,p)
  }

  @Test
  def IS6(): Unit ={
    val q = getQuery("interactive-short-6.cypher")
    val p = Map("messageId" -> "101030792151058")
    run(q,p)
  }

  @Test
  def IS7(): Unit ={
    val q = getQuery("interactive-short-7.cypher")
    val p = Map("messageId" -> "401030792151576")
    run(q,p)
  }

  @Test
  def Q1(): Unit = {
    val q = getQuery("interactive-complex-1.cypher")
    val p = Map("personId" -> "210995116277782", "firstName" -> "Ali")
    run(q,p)
    //shortestPath
  }

  @Test
  def Q2(): Unit = {
    val q = getQuery("interactive-complex-2.cypher")
    val p = Map("personId" -> "210995116277782", "maxDate" -> "1287230400000")
    run(q,p)
  }
//
  @Test
  def Q3(): Unit = {
    val q = getQuery("interactive-complex-3.cypher")
    val p = Map("personId" -> "10995116277794", "countryXName" -> "Angola", "countryYName" -> "Colombia", "startDate" -> "1275393600000", "endDate" -> "1277812800000")
    run(q,p)
  }

  //2 hop
  @Test
  def Q4(): Unit = {
    val q = getQuery("interactive-complex-4.cypher")
    val p = Map("personId" -> "10995116277794", "startDate" -> "1275393600000", "endDate" -> "1277812800000")
    run(q,p)
  }

  @Test
  def Q5(): Unit = {
    val q = getQuery("interactive-complex-5.cypher")
    val p = Map("personId" -> "10995116277794", "minDate" -> "1287230400000")
    run(q,p)
  }

  @Test
  def Q6(): Unit = {
    val q = getQuery("interactive-complex-6.cypher")
    val p = Map("personId" -> "10995116277794", "tagName" -> "Carl_Gustaf_Emil_Mannerheim")
    run(q,p)
  }

  @Test
  def Q7(): Unit = {
    val q = getQuery("interactive-complex-7.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def Q8(): Unit = {
    val q = getQuery("interactive-complex-8.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def Q9(): Unit = {
    val q = getQuery("interactive-complex-9.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def Q10(): Unit = {
    val q = getQuery("interactive-complex-10.cypher")
    val p = Map("personId" -> "10995116277794")
    run(q,p)
  }

  @Test
  def Q11(): Unit = {
    val q = getQuery("interactive-complex-11.cypher")
    val p = Map("personId" -> "10995116277794", "countryName" -> "Hungary", "workFromYear" -> "2011")
    run(q,p)
  }

  // multi hop
  @Test
  def Q12(): Unit = {
    val q = getQuery("interactive-complex-12.cypher")
    val p = Map("personId" -> "10995116277794", "tagName" -> "Carl_Gustaf_Emil_Mannerheim")
    run(q,p)
  }

  //shortestPath
  @Test
  def Q13(): Unit = {
    val q = getQuery("interactive-complex-13.cypher")
    val p = Map("person1Id" -> "10995116277794", "person2Id" -> "8796093022357")
    run(q,p)
  }

  //shortestPath
  @Test
  def Q14(): Unit = {
    val q = getQuery("interactive-complex-14.cypher")
    val p = Map("person1Id" -> "10995116277794", "person2Id" -> "8796093022357")
    run(q,p)
  }

  @Test
  def u1(): Unit = {
    val q = getQuery("interactive-update-1.cypher")
    val p = Map("cityId" -> "500000000000111")
    run(q, p)
  }

}
