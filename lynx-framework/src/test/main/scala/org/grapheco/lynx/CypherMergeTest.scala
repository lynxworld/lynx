package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2021-05-25 14:47
 */
class CypherMergeTest extends TestBase {

  @Test
  def testMergeNode(): Unit ={
    val id = all_nodes.size
    val n1 = runOnDemoGraph("merge (n:coder) return n").records().next()("n").asInstanceOf[LynxNode]
    val n2 = runOnDemoGraph("merge (n:coder2{name:'cat'}) return n").records().next()("n").asInstanceOf[LynxNode]
    val n3 = runOnDemoGraph("merge (n:person{name:'Alice'}) return n")
    val n4 = runOnDemoGraph("match (n) return count(n) as count").records().next()("count").asInstanceOf[LynxValue]
    Assert.assertEquals((id + 2).toLong, n4.value)
    Assert.assertEquals("coder", n1.labels.head)
    Assert.assertEquals("cat", n2.property("name").get.value)

  }

  @Test
  def testMergeSingleNodeDerivedFromExistingNodeProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |match (n:person)
        |merge (m:city{name:n.name}) return m
        |""".stripMargin)
  }

  @Test
  def testOnCreateOnMatch(): Unit ={
    val res1 = runOnDemoGraph(
      """
        MERGE (keanu:Person {name: 'Keanu Reeves'})
        |ON CREATE
        |  SET keanu.created = 2333
        |RETURN keanu
        |""".stripMargin).records().next()("keanu").asInstanceOf[LynxNode]
    Assert.assertEquals(2333L, res1.property("created").get.value)

    val res2 = runOnDemoGraph(
      """
        MERGE (person:Person)
        |ON MATCH
        |  SET person.found = true
        |RETURN person
        |""".stripMargin).records()
    Assert.assertEquals(false, res2.hasNext)

    val res3 = runOnDemoGraph(
      """
        MERGE (keanu:Person {name: 'Keanu Reeves'})
        |ON CREATE
        |  SET keanu.created = timestamp()
        |ON MATCH
        |  SET keanu.lastSeen = timestamp()
        |RETURN keanu.name, keanu.created, keanu.lastSeen
        |""".stripMargin).records()

    val res4 = runOnDemoGraph(
      """
        MERGE (person:Person)
        |ON MATCH
        |  SET
        |    person.found = true,
        |    person.lastAccessed = timestamp()
        |RETURN person.name, person.found, person.lastAccessed
        |""".stripMargin).records()
  }


  @Test
  def testMergeARelationship(): Unit ={
    // TODO: nodeFilter push down
    val relNum = all_rels.size
    val res = runOnDemoGraph(
      """
        |match (n:person{name:'bluejoe'}), (m:person{name:'Alice'})
        |merge (n)-[r:XXX]->(m) return r
        |""".stripMargin).records().next()("r").asInstanceOf[LynxRelationship]
    Assert.assertEquals("XXX", res.relationType.get)

    val res2 = runOnDemoGraph("match (n)-[r]->(m) return count(r) as count").records().next()("count").asInstanceOf[LynxValue]
    Assert.assertEquals(relNum + 1, res2.value)
  }
}
