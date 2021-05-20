package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description: cypher remove impl
 * @author: LiamGao
 * @create: 2021-05-19 16:48
 */
class CypherRemoveTest extends TestBase {
  @Test
  def testRemoveNodeProperty(): Unit ={
    val res = runOnDemoGraph("match (n) where n.name='Bob' remove n.age, n.gender, n.money return n ").records().next()("n").asInstanceOf[TestNode].properties
    Assert.assertEquals(Seq(LynxValue("Bob")), res.values.toSeq)
  }

  @Test
  def testRemoveNodeLabel(): Unit ={
    runOnDemoGraph("create (n:p1:p2:p3:p4{name:'A'})")

    val res1 = runOnDemoGraph("match (n) where n.name='A' remove n:p3 return n").records().next()("n").asInstanceOf[TestNode].labels
    Assert.assertEquals(Seq("p1", "p2", "p4"), res1)

    val res2 = runOnDemoGraph("match (n) where n.name='A' remove n:p2:p4 return n").records().next()("n").asInstanceOf[TestNode].labels
    Assert.assertEquals(Seq("p1"), res2)
  }

  @Test
  def testRemoveRelationshipProperty(): Unit ={
    runOnDemoGraph("match (n)-[r]->(m) where n.name='Alice' set r.value1='tmp', r.value2=100, r.value3=true return r")
    val res = runOnDemoGraph("match (n)-[r]->(m) where n.name='Alice' remove r.value1, r.value3 return r").records().next()("r").asInstanceOf[TestRelationship].properties
    Assert.assertEquals(Seq(LynxValue(100)), res.values.toSeq)
  }

  @Test
  def testRemoveRelationshipType(): Unit ={
    runOnDemoGraph("match (n)-[r]->(m) where n.name='Alice' remove r:KNOWS return r").show()
  }
}
