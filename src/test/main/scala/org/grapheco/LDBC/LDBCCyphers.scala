package org.grapheco.LDBC

object LDBCCyphers {

  val parameters: Map[String, Any] = Map(
    "personId"-> "10995116277795",
    "moderatorPersonId" -> "10995116277795",
    "authorPersonId" -> "10995116277795",
    "person1Id"->"10995116277795",
    "person2Id"->"10995116277795",
    "messageId" -> "1",
    "countryId" -> "2",
    "cityId" -> "1",
    "postId" -> "3",
    "commentId" -> "4",
    "forumId" -> "5",
    "replyToPostId"->"1",
    "replyToCommentId"->"1",
    "personFirstName"->"a",
    "personLastName"->"b",
    "gender"->"a",
    "birthday"->"a",
    "creationDate"->"a",
    "locationIP"->"a",
    "browserUsed"->"a",
    "languages"->"a",
    "emails"->"a",
    "content"->"a",
    "imageFile"->"a",
    "length"->"a",
    "forumTitle" -> "a",
    "tagIds" -> List("1","2","3"),
    "studyAt" -> List("1","2","3"),
    "workAt" -> List("1","2","3"),
  )

  val all: List[String] = List(IS1, IS2, IS3, IS4, IS5, IS6, IS7, IU1, IU2, IU3, IU4, IU5, IU6, IU7, IU8)

  val IS1: String =
    """
      |MATCH (n:Person {id: $personId })-[:IS_LOCATED_IN]->(p:City)
      |RETURN
      |    n.firstName AS firstName,
      |    n.lastName AS lastName,
      |    n.birthday AS birthday,
      |    n.locationIP AS locationIP,
      |    n.browserUsed AS browserUsed,
      |    p.id AS cityId,
      |    n.gender AS gender,
      |    n.creationDate AS creationDate
      |""".stripMargin

  val IS2 =
    """
      |MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
      |WITH
      | message,
      | message.id AS messageId,
      | message.creationDate AS messageCreationDate
      |ORDER BY messageCreationDate DESC, messageId ASC
      |LIMIT 10
      |MATCH (message)-[:REPLY_OF*0..]->(post:Post),
      |      (post)-[:HAS_CREATOR]->(person)
      |RETURN
      | messageId,
      | coalesce(message.imageFile,message.content) AS messageContent,
      | messageCreationDate,
      | post.id AS postId,
      | person.id AS personId,
      | person.firstName AS personFirstName,
      | person.lastName AS personLastName
      |ORDER BY messageCreationDate DESC, messageId ASC
      |""".stripMargin

  val IS3 =
    """
      |MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
      |RETURN
      |    friend.id AS personId,
      |    friend.firstName AS firstName,
      |    friend.lastName AS lastName,
      |    r.creationDate AS friendshipCreationDate
      |ORDER BY
      |    friendshipCreationDate DESC,
      |    toInteger(personId) ASC
      |""".stripMargin

  val IS4 =
    """
      |MATCH (m:Message {id:  $messageId })
      |RETURN
      |    m.creationDate as messageCreationDate,
      |    coalesce(m.content, m.imageFile) as messageContent
      |""".stripMargin

  val IS5 =
    """
      |MATCH (m:Message {id:  $messageId })-[:HAS_CREATOR]->(p:Person)
      |RETURN
      |    p.id AS personId,
      |    p.firstName AS firstName,
      |    p.lastName AS lastName
      |
      |""".stripMargin

  val IS6 =
    """
      |MATCH (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
      |RETURN
      |    f.id AS forumId,
      |    f.title AS forumTitle,
      |    mod.id AS moderatorId,
      |    mod.firstName AS moderatorFirstName,
      |    mod.lastName AS moderatorLastName
      |""".stripMargin

  val IS7 =
    """
      |MATCH (m:Message {id: $messageId })<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
      |    OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
      |    RETURN c.id AS commentId,
      |        c.content AS commentContent,
      |        c.creationDate AS commentCreationDate,
      |        p.id AS replyAuthorId,
      |        p.firstName AS replyAuthorFirstName,
      |        p.lastName AS replyAuthorLastName,
      |        CASE
      |            WHEN r IS NULL THEN false
      |            ELSE true
      |        END AS replyAuthorKnowsOriginalMessageAuthor
      |    ORDER BY commentCreationDate DESC, replyAuthorId
      |
      |""".stripMargin

  val IU1 =
    """
      |MATCH (c:City {id: $cityId})
      |CREATE (p:Person {
      |    id: $personId,
      |    firstName: $personFirstName,
      |    lastName: $personLastName,
      |    gender: $gender,
      |    birthday: $birthday,
      |    creationDate: $creationDate,
      |    locationIP: $locationIP,
      |    browserUsed: $browserUsed,
      |    languages: $languages,
      |    email: $emails
      |  })-[:IS_LOCATED_IN]->(c)
      |WITH p, count(*) AS dummy1
      |UNWIND $tagIds AS tagId
      |  MATCH (t:Tag {id: tagId})
      |  CREATE (p)-[:HAS_INTEREST]->(t)
      |WITH p, count(*) AS dummy2
      |UNWIND $studyAt AS s
      |  MATCH (u:University {id: s[0]})
      |  CREATE (p)-[:STUDY_AT {classYear: s[1]}]->(u)
      |WITH p, count(*) AS dummy3
      |UNWIND $workAt AS w
      |  MATCH (comp:Company {id: w[0]})
      |  CREATE (p)-[:WORKS_AT {workFrom: w[1]}]->(comp)
      |""".stripMargin

  val IU2 =
    """
      |MATCH (person:Person {id: $personId}), (post:Post {id: $postId})
      |CREATE (person)-[:LIKES {creationDate: $creationDate}]->(post)
      |
      |""".stripMargin

  val IU3 =
    """
      |
      |MATCH (person:Person {id: $personId}), (comment:Comment {id: $commentId})
      |CREATE (person)-[:LIKES {creationDate: $creationDate}]->(comment)
      |""".stripMargin

  val IU4 =
    """
      |MATCH (p:Person {id: $moderatorPersonId})
      |CREATE (f:Forum {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:HAS_MODERATOR]->(p)
      |WITH f
      |UNWIND $tagIds AS tagId
      |  MATCH (t:Tag {id: tagId})
      |  CREATE (f)-[:HAS_TAG]->(t)
      |""".stripMargin

  val IU5 =
    """
      |MATCH (f:Forum {id: $forumId}), (p:Person {id: $personId})
      |CREATE (f)-[:HAS_MEMBER {creationDate: $creationDate}]->(p)
      |
      |""".stripMargin

  val IU6 =
    """
      |MATCH (author:Person {id: $authorPersonId}), (country:Country {id: $countryId}), (forum:Forum {id: $forumId})
      |CREATE (author)<-[:HAS_CREATOR]-(p:Post:Message {
      |    id: $postId,
      |    creationDate: $creationDate,
      |    locationIP: $locationIP,
      |    browserUsed: $browserUsed,
      |    language: $language,
      |    content: CASE $content WHEN '' THEN NULL ELSE $content END,
      |    imageFile: CASE $imageFile WHEN '' THEN NULL ELSE $imageFile END,
      |    length: $length
      |  })<-[:CONTAINER_OF]-(forum), (p)-[:IS_LOCATED_IN]->(country)
      |WITH p
      |UNWIND $tagIds AS tagId
      |  MATCH (t:Tag {id: tagId})
      |  CREATE (p)-[:HAS_TAG]->(t)
      |""".stripMargin

  val IU7 =
    """
      |MATCH
      |  (author:Person {id: $authorPersonId}),
      |  (country:Country {id: $countryId}),
      |  (message:Message {id: $replyToPostId + $replyToCommentId})
      |CREATE (author)<-[:HAS_CREATOR]-(c:Comment:Message {
      |    id: $commentId,
      |    creationDate: $creationDate,
      |    locationIP: $locationIP,
      |    browserUsed: $browserUsed,
      |    content: $content,
      |    length: $length
      |  })-[:REPLY_OF]->(message),
      |  (c)-[:IS_LOCATED_IN]->(country)
      |WITH c
      |UNWIND $tagIds AS tagId
      |  MATCH (t:Tag {id: tagId})
      |  CREATE (c)-[:HAS_TAG]->(t)
      |
      |""".stripMargin

  val IU8 =
    """
      |MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
      |CREATE (p1)-[:KNOWS {creationDate: $creationDate}]->(p2)
      |""".stripMargin

}
