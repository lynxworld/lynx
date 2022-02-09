package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-09 13:55
 */
class CypherRemoveTest extends TestBase {
  @Test
  def removeNodeProperty(): Unit ={
    runOnDemoGraph("create (n:person:worker{name:'Oliver Stone', sex:'male', age:18, school:'ABC'})")
    val res = runOnDemoGraph("match (n:person{name:'Oliver Stone'}) remove n.school return n").records().next()
    Assert.assertEquals(Seq("Oliver Stone", "male", 18), res("n").asInstanceOf[TestNode].props.values.toSeq.map(_.value))
  }

  @Test
  def removeNodeLabel(): Unit ={
    runOnDemoGraph("create (n:person:worker{name:'Oliver Stone', sex:'male', age:18, school:'ABC'})")
    val res = runOnDemoGraph("match (n:person{name:'Oliver Stone'}) remove n:worker return n").records().next()
    Assert.assertEquals(Seq("person"), res("n").asInstanceOf[TestNode].labels.map(_.value))
  }

  @Test
  def removeRelationshipProperties(): Unit ={
    runOnDemoGraph("create (n:person{name:'A'})-[r:KNOW{year:2000, month:5, day:20}]->(m:person{name:'B'})")
    val res = runOnDemoGraph("match (n)-[r]->(m) remove r.year return r").records().next()
    Assert.assertEquals(Seq(5, 20), res("r").asInstanceOf[TestRelationship].props.values.map(_.value).toSeq)
  }
}
