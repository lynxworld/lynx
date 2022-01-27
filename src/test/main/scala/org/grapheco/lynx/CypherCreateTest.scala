package org.grapheco.lynx

import org.junit.{Assert, Test}
import org.grapheco.lynx.NameParser._
class CypherCreateTest extends TestBase {
  runOnDemoGraph(
    """
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c{name:"Bob", age: 10, gender:"male"}),
      |(d{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)
  val NODE_SIZE: Int = all_nodes.size
  val REL_SIZE: Int = all_rels.size
  @Test
  def testCreateNode(): Unit = {
    var rs = runOnDemoGraph("CREATE ()")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)

    rs = runOnDemoGraph("CREATE (n)")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)

    //should invoke CREATE even if result not retrieved
    runner.run("CREATE (n)", Map.empty)
    Assert.assertEquals(NODE_SIZE + 3, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
  }

  @Test
  def testCreate2Nodes(): Unit = {
    var rs = runOnDemoGraph("CREATE (),()")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)

    rs = runOnDemoGraph("CREATE (n),(m)")
    Assert.assertEquals(NODE_SIZE + 4, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)

    //should invoke CREATE even if result not retrieved
    runner.run("CREATE (n),(m)", Map.empty)
    Assert.assertEquals(NODE_SIZE + 6, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
  }

  @Test
  def testCreateRelation(): Unit = {
    var rs = runOnDemoGraph("CREATE ()-[:KNOWS]->()")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)
  }

  @Test
  def testCreate2Relations(): Unit = {
    var rs = runOnDemoGraph("CREATE ()-[:KNOWS]->(),()-[:KNOWS]->()")
    Assert.assertEquals(NODE_SIZE + 4, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)
  }

  @Test
  def testCreate2RelationsInChain(): Unit = {
    var rs = runOnDemoGraph("CREATE ()-[:KNOWS]->()-[:KNOWS]->()")
    Assert.assertEquals(NODE_SIZE + 3, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)
  }

  @Test
  def testCreateNamedRelationsInChain(): Unit = {
    var rs = runOnDemoGraph("CREATE (m)-[:KNOWS]->(n)-[:KNOWS]->(t)")
    Assert.assertEquals(NODE_SIZE + 3, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)
  }

  @Test
  def testCreateNamedExistingNodeInChain(): Unit = {
    var rs = runOnDemoGraph("CREATE (m)-[:KNOWS]->(n)-[:KNOWS]->(m)")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)
  }

  @Test
  def testCreateNodeWithproperty(): Unit = {
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000})")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
    Assert.assertEquals(LynxString("God"), all_nodes.apply(NODE_SIZE).property("name").get)
  }

  @Test
  def testCreateNodeWithReturn(): Unit = {
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000}) return n")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
    Assert.assertEquals((NODE_SIZE + 1).toLong, rs.records().toSeq.head.apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testCreateNodesRelation(): Unit = {
    val rs = runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000}), (m:place {name: 'heaven'}), (n)-[r:livesIn]->(m) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).property("name").get)
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).property("age").get)
    Assert.assertEquals(Seq("person"), all_nodes(NODE_SIZE).labels.map(_.value))

    Assert.assertEquals(LynxString("heaven"), all_nodes(NODE_SIZE + 1).property("name").get)
    Assert.assertEquals(Seq("place"), all_nodes(NODE_SIZE + 1).labels.map(_.value))

    Assert.assertEquals("livesIn", all_rels(REL_SIZE).relationType.get.value)
    Assert.assertEquals(all_nodes(NODE_SIZE).id.value, all_rels(REL_SIZE).startNodeId.value)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endNodeId.value)
  }

  @Test
  def testCreateNodesAndRelationsWithinPath(): Unit = {
    val rs = runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000})-[r:livesIn]->(m:place {name: 'heaven'}) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).property("name").get)
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).property("age").get)
    Assert.assertEquals(Seq("person"), all_nodes(NODE_SIZE).labels.map(_.value))

    Assert.assertEquals(LynxString("heaven"), all_nodes(NODE_SIZE + 1).property("name").get)
    Assert.assertEquals(Seq("place"), all_nodes(NODE_SIZE + 1).labels.map(_.value))

    Assert.assertEquals("livesIn", all_rels(REL_SIZE).relationType.get.value)
    Assert.assertEquals(all_nodes(NODE_SIZE).id.value, all_rels(REL_SIZE).startNodeId.value)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endNodeId.value)
  }

  @Test
  def testCreateNodesPath(): Unit = {
    val rs = runOnDemoGraph("CREATE (a:person {name: 'BaoChai'}), (b:person {name: 'BaoYu'}), (c:person {name: 'DaiYu'}), (a)-[:LOVES]->(b)-[:LOVES]->(c) return a,b,c")
    Assert.assertEquals(NODE_SIZE + 3, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)

    Assert.assertEquals(LynxString("BaoChai"), all_nodes(NODE_SIZE).property("name").get)
    Assert.assertEquals(LynxString("BaoYu"), all_nodes(NODE_SIZE + 1).property("name").get)
    Assert.assertEquals(LynxString("DaiYu"), all_nodes(NODE_SIZE + 2).property("name").get)

    Assert.assertEquals("LOVES", all_rels(REL_SIZE).relationType.get.value)
    Assert.assertEquals(all_nodes(NODE_SIZE).id.value, all_rels(REL_SIZE).startNodeId.value)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endNodeId.value)

    Assert.assertEquals("LOVES", all_rels(REL_SIZE + 1).relationType.get.value)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE + 1).startNodeId.value)
    Assert.assertEquals(all_nodes(NODE_SIZE + 2).id.value, all_rels(REL_SIZE + 1).endNodeId.value)
  }

  @Test
  def testMatchToCreateMultipleNodesAndRelations(): Unit = {
    var rs = runOnDemoGraph("match (m:person) CREATE (n {name: 'God', age: 10000}), (n)-[r:LOVES]->(m) return n,r,m").show()
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).property("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).property("age"))

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE + 1).property("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE + 1).property("age"))

    Assert.assertEquals("LOVES", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals((NODE_SIZE + 1).toLong, all_rels(REL_SIZE).startNodeId)
    Assert.assertEquals(1.toLong, all_rels(REL_SIZE).endNodeId)

    Assert.assertEquals("LOVES", all_rels(REL_SIZE + 1).relationType.get)
    Assert.assertEquals((NODE_SIZE + 2).toLong, all_rels(REL_SIZE + 1).startNodeId)
    Assert.assertEquals(2.toLong, all_rels(REL_SIZE + 1).endNodeId)
  }

  @Test
  def testCreateIndex(): Unit = {
    runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000}), (m:place {name: 'heaven'}), (n)-[r:livesIn]->(m) return n,r,m")
    runOnDemoGraph("CREATE INDEX ON :person(name)")
    runOnDemoGraph("CREATE INDEX ON :person(name, age)")
  }


  @Test
  def testCreateDate(): Unit ={
    runOnDemoGraph("CREATE (n:Person {name:'node_Date1',born:date('2018-04-05')}) return n")
  }
}
