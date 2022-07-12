package org.grapheco.LDBC

import org.grapheco.LDBC.LDBCQueryTest.ldbcTestBase
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.time.LynxDate
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

  val update_person_ids = List("219791209300010", "226388279066632", "226388279066641", "210995116277761", "200000000000014", "200000000000016", "204398046511148", "215393162788899", "226388279066650", "221990232555526", "221990232555527", "200000000000027", "215393162788910", "210995116277782", "215393162788912", "200000000000033", "210995116277783", "226388279066664", "232985348833291", "200000000000047", "228587302322180", "202199023255557", "232985348833319", "228587302322191", "228587302322196")
  val update_post_id = List("101030792151040", "101030792151041", "101030792151042", "101030792151043", "101030792151044", "101030792151045", "101030792151046", "101030792151047", "101030792151048", "101030792151049", "101030792151050", "101030792151051", "101030792151052", "101030792151053", "101030792151054", "101030792151055", "101030792151056", "101030792151057", "101030792151058", "100962072674323", "100962072674324", "100962072674325", "100962072674326", "100962072674327", "100962072674328")


  @Test
  def u2(): Unit ={
    val q = getQuery("interactive-update-2.cypher")
    val p = Map("personId" -> update_person_ids(0),
      "postId" -> update_post_id(0),
      "creationDate" -> LynxDate.today)
    run(q, p)
    val verify = "MATCH (person:Person {id: $personId})-[r:LIKES]-(post:Post {id: $postId}) return r"
    val result = ldbcTestBase.run(verify, p)
    result.records()
  }

}
