package org.grapheco.LDBC

import org.grapheco.LDBC.LDBCQueryTest.{ldbcTestBase, personIds}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.time.LynxDate
import org.grapheco.lynx.util.Profiler
import org.junit.{Assert, BeforeClass, Test}

import java.io.File
import java.time.LocalDate
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

  val update_person_id = List("219791209300010", "226388279066632", "226388279066641", "210995116277761", "200000000000014", "200000000000016", "204398046511148", "215393162788899", "226388279066650", "221990232555526", "221990232555527", "200000000000027", "215393162788910", "210995116277782", "215393162788912", "200000000000033", "210995116277783", "226388279066664", "232985348833291", "200000000000047", "228587302322180", "202199023255557", "232985348833319", "228587302322191", "228587302322196")
  val update_post_id = List("101030792151040", "101030792151041", "101030792151042", "101030792151043", "101030792151044", "101030792151045", "101030792151046", "101030792151047", "101030792151048", "101030792151049", "101030792151050", "101030792151051", "101030792151052", "101030792151053", "101030792151054", "101030792151055", "101030792151056", "101030792151057", "101030792151058", "100962072674323", "100962072674324", "100962072674325", "100962072674326", "100962072674327", "100962072674328")
  val update_comment_id = List("401030792151558", "401030792151559", "401030792151560", "401030792151561", "401030792151562", "401030792151563", "401030792151564", "401030792151565", "401030792151566", "401030792151567", "401030792151568", "401030792151569", "401030792151570", "401030792151571", "401030792151572", "401030792151573", "401030792151575", "401030792151576", "401030792151577", "401030792151578", "401030792151579", "401030792151580", "401030792151581", "401030792151582", "401030792151583")
  val update_forum_id = List("300687194767360", "300962072674305", "301030792151042", "300962072674307", "300687194767364", "300824633720838", "300824633720840", "300893353197579", "300962072674316", "300893353197581", "300893353197583", "300068719476752", "300481036337170", "300000000000019", "300206158430228", "300343597383701", "300412316860438", "300549755813911", "300687194767384", "300343597383706", "300481036337179", "300824633720860", "300618475290653", "300068719476766", "300137438953503")
  val update_country_id = List("500000000000000", "500000000000001", "500000000000002", "500000000000003", "500000000000004", "500000000000005", "500000000000006", "500000000000007", "500000000000008", "500000000000009", "500000000000010", "500000000000011", "500000000000012", "500000000000013", "500000000000014", "500000000000015", "500000000000016", "500000000000017", "500000000000018", "500000000000019", "500000000000020", "500000000000021", "500000000000022", "500000000000023", "500000000000024")
  val update_tag_id = List("700000000000000", "700000000000001", "700000000000002", "700000000000003", "700000000000004", "700000000000005", "700000000000006", "700000000000007", "700000000000008", "700000000000009", "700000000000010", "700000000000011", "700000000000012", "700000000000013", "700000000000014", "700000000000015", "700000000000016", "700000000000017", "700000000000018", "700000000000019", "700000000000020", "700000000000021", "700000000000022", "700000000000023", "700000000000024")

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
    val p = Map("personId" -> update_person_id(0), "firstName"-> "Jose")
    run(q,p)
  }

  @Test
  def Q2(): Unit = {
    val q = getQuery("interactive-complex-2.cypher")
    val p = Map("personId" -> update_person_id(0), "maxDate" -> LynxDate.today)
    run(q,p)
  }

  @Test
  def Q3(): Unit = {
    val q = getQuery("interactive-complex-3.cypher")
    val p = Map("personId" -> update_person_id(0),
      "countryXName" -> "Angola",
      "countryYName" -> "Colombia",
      "startDate" -> LynxDate(LocalDate.of(1800,1,1)),
      "endDate" -> LynxDate.today)
    run(q,p)
  }

  @Test
  def Q4(): Unit = {
    val q = getQuery("interactive-complex-4.cypher")
    val p = Map("personId" -> update_person_id(0),
      "startDate" -> LynxDate(LocalDate.of(1800,1,1)),
      "endDate" -> LynxDate.today)
    run(q,p)
  }

  @Test
  def Q5(): Unit = {
    val q = getQuery("interactive-complex-5.cypher")
    val p = Map("personId" -> update_person_id(0), "minDate" -> LynxDate(LocalDate.of(1800,1,1)))
    run(q,p)
  }

  @Test
  def Q6(): Unit = {
    val q = getQuery("interactive-complex-6.cypher")
    val p = Map("personId" -> update_person_id(0), "tagName" -> "Carl_Gustaf_Emil_Mannerheim")
    run(q,p)
  }

  @Test
  def Q7(): Unit = {
    val q = getQuery("interactive-complex-7.cypher")
    val p = Map("personId" -> update_person_id(0))
    run(q,p)
  }

  @Test
  def Q8(): Unit = {
    val q = getQuery("interactive-complex-8.cypher")
    val p = Map("personId" -> update_person_id(0))
    run(q,p)
  }

  @Test
  def Q9(): Unit = {
    val q = getQuery("interactive-complex-9.cypher")
    val p = Map("personId" -> update_person_id(0), "maxDate" -> LynxDate.today)
    run(q,p)
  }

  @Test
  def Q10(): Unit = {
    val q = getQuery("interactive-complex-10.cypher")
    val p = Map("personId" -> update_person_id(0), "month" -> 5)
    run(q,p)
  }

  @Test
  def Q11(): Unit = {
    val q = getQuery("interactive-complex-11.cypher")
    val p = Map("personId" -> update_person_id(0),
      "countryName" -> "Hungary",
      "workFromYear" -> "2011")
    run(q,p)
  }

  @Test
  def Q12(): Unit = {
    val q = getQuery("interactive-complex-12.cypher")
    val p = Map("personId" -> update_person_id(0), "tagClassName" -> "Carl_Gustaf_Emil_Mannerheim")
    run(q,p)
  }

  @Test
  def Q13(): Unit = {
    val q = getQuery("interactive-complex-13.cypher")
    val p = Map("person1Id" -> update_person_id(0), "person2Id" -> update_person_id(2))
    run(q,p)
  }

  @Test
  def Q14(): Unit = {
    val q = getQuery("interactive-complex-14.cypher")
    val p = Map("person1Id" -> update_person_id(0), "person2Id" -> update_person_id(2))
    run(q,p)
  }

  @Test
  def u1(): Unit = {
    val q = getQuery("interactive-update-1.cypher")
    val p = Map("cityId" -> "500000000000111", "personId" -> update_person_id(0),
      "personFirstName" -> "Bob", "personLastName" -> "Green",
      "gender" -> "Male", "birthday" -> "1995-06-06",
      "creationDate" -> "2020-03-28", "locationIP" -> "10.0.88.88",
      "browserUsed" -> "Chrome", "languages" -> "Chinese",
      "emails" -> "bobgreem@gmail.com", "tagIds" -> update_comment_id,
      "studyAt" -> update_person_id, "workAt" -> update_post_id
    )
    run(q, p)
  }


  @Test
  def u2(): Unit ={
    val q = getQuery("interactive-update-2.cypher")
    val p = Map("personId" -> update_person_id(0),
      "postId" -> update_post_id(0),
      "creationDate" -> LynxDate.today)
    run(q, p)
    val verify = "MATCH (person:Person {id: $personId})-[r:LIKES]->(post:Post {id: $postId}) return r"
    val result = ldbcTestBase.runner.run(verify, p)
    Assert.assertTrue(
      result.records()
      .flatMap(_.getAsRelationship("r"))
      .flatMap(_.property(LynxPropertyKey("creationDate")))
      .map(_.asInstanceOf[LynxDate]).exists(LynxDate.today.equals))
  }

  @Test
  def u3(): Unit ={
    val q = getQuery("interactive-update-3.cypher")
    val p = Map("personId" -> update_person_id(1),
      "commentId" -> update_comment_id(0),
      "creationDate" -> LynxDate.today)
    run(q, p)
    val verify = "MATCH (person:Person {id: $personId})-[r:LIKES]->(comment:Comment {id: $commentId}) return r"
    val result = ldbcTestBase.runner.run(verify, p)
    Assert.assertTrue(
      result.records()
        .flatMap(_.getAsRelationship("r"))
        .flatMap(_.property(LynxPropertyKey("creationDate")))
        .map(_.asInstanceOf[LynxDate]).exists(LynxDate.today.equals))
  }

  @Test
  def u4(): Unit ={
    val q =
      """
        |MATCH (p:Person {id: $moderatorPersonId})
        |CREATE (f:Forum {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:HAS_MODERATOR]->(p)
        |WITH f
        |UNWIND $tagIds AS tagId
        |  MATCH (t:Tag {id: tagId})
        |  CREATE (f)-[:HAS_TAG]->(t)
        |""".stripMargin
    val p = Map("moderatorPersonId" -> update_person_id(0),
      "forumId" -> update_forum_id(0),
      "creationDate" -> LynxDate.today,
      "tagIds" -> update_post_id,
      "forumTitle" -> "TestTitle")
    run(q, p)
  }

  @Test
  def u5(): Unit ={
    val q = getQuery("interactive-update-5.cypher")
    val p = Map("personId" -> update_person_id(2),
      "forumId" -> update_forum_id(0),
      "joinDate" -> LynxDate.today)
    run(q, p)
    val verify = "MATCH (f:Forum {id: $forumId})-[r:HAS_MEMBER]->(p:Person {id: $personId}) return r"
    val result = ldbcTestBase.runner.run(verify, p)
    Assert.assertTrue(
      result.records()
        .flatMap(_.getAsRelationship("r"))
        .flatMap(_.property(LynxPropertyKey("joinDate")))
        .map(_.asInstanceOf[LynxDate]).exists(LynxDate.today.equals))
  }

  @Test
  def u6(): Unit = {
    val q = getQuery("interactive-update-6.cypher")
    val p = Map("authorPersonId" -> update_person_id(0),
      "countryId" -> update_country_id(0),
      "forumId" -> update_forum_id(0),
      "postId" -> "111",
      "creationDate" -> LynxDate.today,
      "locationIP" -> "10.0.88.88",
      "browserUsed" -> "Chrome",
      "content" -> "LOL",
      "imageFile" -> "",
      "length" -> 3,
      "tagIds" -> update_tag_id
    )
    run(q, p)
  }

  @Test
  def u7(): Unit = {
    val q = getQuery("interactive-update-7.cypher")
    val p = Map("authorPersonId" -> update_person_id(0),
      "countryId" -> update_country_id(0),
      "replyToPostId" -> "1010307921",
      "replyToCommentId" -> "51040",
      "forumId" -> update_forum_id(0),
      "commentId" -> "111",
      "creationDate" -> LynxDate.today,
      "locationIP" -> "10.0.88.88",
      "browserUsed" -> "Chrome",
      "content" -> "LOL",
      "length" -> 3,
      "tagIds" -> update_tag_id
    )
    ldbcTestBase.run(q, p)
  }

  @Test
  def u8(): Unit ={
    val q = getQuery(  "interactive-update-8.cypher")
    val p = Map(
      "person1Id" -> update_person_id(8),
      "person2Id" -> update_person_id(9),
      "creationDate" -> LynxDate.today)
    run(q, p)
    val verify = "MATCH (p1:Person {id: $person1Id})-[r:KNOWS]->(p2:Person {id: $person2Id}) return r"
    val result = ldbcTestBase.runner.run(verify, p)
    Assert.assertTrue(
      result.records()
        .flatMap(_.getAsRelationship("r"))
        .flatMap(_.property(LynxPropertyKey("creationDate")))
        .map(_.asInstanceOf[LynxDate]).exists(LynxDate.today.equals))
  }
}
