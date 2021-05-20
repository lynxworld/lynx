package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description: cypher set impl
 * @author: LiamGao
 * @create: 2021-05-17 16:29
 */
class CypherSetTest extends TestBase {

  @Test
  def testSetNodeProperty(): Unit ={
    // different style of set
    var record = runOnDemoGraph("match (n) where n.name='Bob' set n += {name:'bob2', age:11} return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("bob2", record.property("name").get.value)
    Assert.assertEquals(11L, record.property("age").get.value)

    record = runOnDemoGraph("match (n) where n.name='bob2' set n.name='bob3' set n.age=22 return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("bob3", record.property("name").get.value)
    Assert.assertEquals(22L, record.property("age").get.value)

    record = runOnDemoGraph("match (n) where n.name='bob3' set n.name='bob4', n.age=33 return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("bob4", record.property("name").get.value)
    Assert.assertEquals(33L, record.property("age").get.value)

    record = runOnDemoGraph("match (n) where n.name='bob4' set n.name='bob5' return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("bob5", record.property("name").get.value)
  }

  @Test
  def testSetNodeLabel(): Unit ={
    var record = runOnDemoGraph("match (n) where n.name='Bob' set n:KKK:QQQ return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals(Seq("KKK", "QQQ"), record.labels)

    record = runOnDemoGraph("match (n) where n.name='Bob' set n:AAA return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals(Seq("KKK", "QQQ", "AAA"), record.labels)
  }

  @Test
  def testSetRelationshipProperty(): Unit ={
    val record = runOnDemoGraph("match (n)-[r]->(m) set r.name='Friend', r.age=10 return r").records().next()("r").asInstanceOf[LynxRelationship]
    Assert.assertEquals("Friend", record.property("name").get.value)
    Assert.assertEquals(10L, record.property("age").get.value)
  }

  @Test
  def testSetRelationshipType(): Unit ={
    val record = runOnDemoGraph("match (n)-[r]->(m) set r:OK return r").records().next()("r").asInstanceOf[LynxRelationship]
    Assert.assertEquals(Option("OK"), record.relationType)
  }
}
