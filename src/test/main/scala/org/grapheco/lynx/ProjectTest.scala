package org.grapheco.lynx

import org.grapheco.LDBC.{LDBCQueryTest, LDBCTestBase}
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}
class ProjectTest extends TestBase {

//  def runOnDemoGraph(str: String) = run(str, Map.empty)

  @Test
  def filteringCombinations(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (p:Person)
        |WHERE p.firstName = 'Ali' OR p.firstName = 'Ken'
        |RETURN p.firstName
         |""".stripMargin)
  }


  @Test
  def bug(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (n)
        |WITH n, [(n)--(m) | m] AS connected_nodes
        |RETURN n, size(connected_nodes) AS num_edges
        |ORDER BY num_edges DESC
        |LIMIT 10
        |""".stripMargin)
  }

  @Test
  def applyBug(): Unit = {
    runOnDemoGraph(
      """
        |MATCH (:Person {id: 21990232611663})<-[:HAS_CREATOR]-(message)-[:HAS_REPLY]->(other)
        |RETURN message,other
        |""".stripMargin)
  }

  @Test
  def procedureBug(): Unit = {
    runOnDemoGraph(
      """
        |UNWIND ['1','3','10','0','-123'] as n
        |RETURN n
        |ORDER BY ToInteger(n)
        |""".stripMargin)
  }

  @Test
  def optionalTest(): Unit = {
//    runOnDemoGraph(
//      """
//        |CREATE (:Person{name:'a'})-[:knows]->(:Person{name:'b'})-[:knows]->(:Author{name:'c'})
//        |""".stripMargin)
    runOnDemoGraph(
      """
        |MATCH (a:Person)-[:knows]-(b:Person)
        |optional MATCH (b)-[:knows]->(c:Author)
        |return a,b,c
        |""".stripMargin)
  }

  @Test
  def test14(): Unit = {
    runOnDemoGraph(
      """
        |match (m:person),(n:Movie)
        |where m.name=n.title
        |return m,n
        |""".stripMargin)
  }


}
