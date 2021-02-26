package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherCreateTest extends TestBase {
  @Test
  def testCreateNode(): Unit = {
    var rs = runOnDemoGraph("CREATE ()")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)

    rs = runOnDemoGraph("CREATE (n)")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
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
  def testCreateNodeWithProperties(): Unit = {
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000})")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
    Assert.assertEquals(LynxValue("God"), all_nodes.apply(NODE_SIZE).property("name").get)
  }

  @Test
  def testCreateNodeWithReturn(): Unit = {
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000}) return n")
    Assert.assertEquals(NODE_SIZE + 1, all_nodes.size)
    Assert.assertEquals(REL_SIZE, all_rels.size)
    Assert.assertEquals((NODE_SIZE + 1).toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testCreateNodesRelation(): Unit = {
    val rs = runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000}), (m:place {name: 'heaven'}), (n)-[r:livesIn]->(m) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).properties("age"))
    Assert.assertEquals(Seq("person"), all_nodes(NODE_SIZE).labels)

    Assert.assertEquals(LynxString("heaven"), all_nodes(NODE_SIZE + 1).properties("name"))
    Assert.assertEquals(Seq("place"), all_nodes(NODE_SIZE + 1).labels)

    Assert.assertEquals("livesIn", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).startId)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endId)
  }

  @Test
  def testCreateNodesAndRelationsWithinPath(): Unit = {
    val rs = runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000})-[r:livesIn]->(m:place {name: 'heaven'}) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).properties("age"))
    Assert.assertEquals(Seq("person"), all_nodes(NODE_SIZE).labels)

    Assert.assertEquals(LynxString("heaven"), all_nodes(NODE_SIZE + 1).properties("name"))
    Assert.assertEquals(Seq("place"), all_nodes(NODE_SIZE + 1).labels)

    Assert.assertEquals("livesIn", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).startId)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endId)
  }

  @Test
  def testCreateNodesPath(): Unit = {
    val rs = runOnDemoGraph("CREATE (a:person {name: 'BaoChai'}), (b:person {name: 'BaoYu'}), (c:person {name: 'DaiYu'}), (a)-[:LOVES]->(b)-[:LOVES]->(c) return a,b,c")
    Assert.assertEquals(NODE_SIZE + 3, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)

    Assert.assertEquals(LynxString("BaoChai"), all_nodes(NODE_SIZE).properties("name"))
    Assert.assertEquals(LynxString("BaoYu"), all_nodes(NODE_SIZE + 1).properties("name"))
    Assert.assertEquals(LynxString("DaiYu"), all_nodes(NODE_SIZE + 2).properties("name"))

    Assert.assertEquals("LOVES", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals(all_nodes(NODE_SIZE).id.value, all_rels(REL_SIZE).startId)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE).endId)

    Assert.assertEquals("LOVES", all_rels(REL_SIZE + 1).relationType.get)
    Assert.assertEquals(all_nodes(NODE_SIZE + 1).id.value, all_rels(REL_SIZE + 1).startId)
    Assert.assertEquals(all_nodes(NODE_SIZE + 2).id.value, all_rels(REL_SIZE + 1).endId)
  }

  @Test
  def testMatchToCreateRelation(): Unit = {
    var rs = runOnDemoGraph("match (m:person {name:'bluejoe'}),(n {name:'CNIC'}) CREATE (m)-[r:WORKS_FOR]->(n) return m,r,n")
    Assert.assertEquals(NODE_SIZE, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 1, all_rels.size)

    Assert.assertEquals("WORKS_FOR", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals(1.toLong, all_rels(REL_SIZE).startId)
    Assert.assertEquals(3.toLong, all_rels(REL_SIZE).endId)
  }

  @Test
  def testMatchToCreateMultipleRelation(): Unit = {
    var rs = runOnDemoGraph("match (m:person),(n {name:'CNIC'}) CREATE (m)-[r:WORKS_FOR]->(n) return m,r,n")
    Assert.assertEquals(NODE_SIZE, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size) //2 persons

    Assert.assertEquals("WORKS_FOR", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals(1.toLong, all_rels(REL_SIZE).startId)
    Assert.assertEquals(3.toLong, all_rels(REL_SIZE).endId)
  }

  @Test
  def testMatchToCreateMultipleNodesAndRelations(): Unit = {
    var rs = runOnDemoGraph("match (m:person) CREATE (n {name: 'God', age: 10000}), (n)-[r:LOVES]->(m) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE).properties("age"))

    Assert.assertEquals(LynxString("God"), all_nodes(NODE_SIZE + 1).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(NODE_SIZE + 1).properties("age"))

    Assert.assertEquals("LOVES", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals((NODE_SIZE + 1).toLong, all_rels(REL_SIZE).startId)
    Assert.assertEquals(1.toLong, all_rels(REL_SIZE).endId)

    Assert.assertEquals("LOVES", all_rels(REL_SIZE + 1).relationType.get)
    Assert.assertEquals((NODE_SIZE + 2).toLong, all_rels(REL_SIZE + 1).startId)
    Assert.assertEquals(2.toLong, all_rels(REL_SIZE + 1).endId)
  }

  @Test
  def testMatchToCreateMultipleNodesAndRelationsWithExpr(): Unit = {
    var rs = runOnDemoGraph("match (m:person) CREATE (n {name: 'clone of '+m.name, age: m.age+1}), (n)-[r:IS_CLONE_OF]->(m) return n,r,m")
    Assert.assertEquals(NODE_SIZE + 2, all_nodes.size)
    Assert.assertEquals(REL_SIZE + 2, all_rels.size)

    Assert.assertEquals(LynxString("clone of bluejoe"), all_nodes(NODE_SIZE).properties("name"))
    Assert.assertEquals(LynxInteger(41), all_nodes(NODE_SIZE).properties("age"))

    Assert.assertEquals(LynxString("clone of alex"), all_nodes(NODE_SIZE + 1).properties("name"))
    Assert.assertEquals(LynxInteger(31), all_nodes(NODE_SIZE + 1).properties("age"))

    Assert.assertEquals("IS_CLONE_OF", all_rels(REL_SIZE).relationType.get)
    Assert.assertEquals((NODE_SIZE + 1).toLong, all_rels(REL_SIZE).startId)
    Assert.assertEquals(1.toLong, all_rels(REL_SIZE).endId)
    Assert.assertEquals("IS_CLONE_OF", all_rels(REL_SIZE + 1).relationType.get)
    Assert.assertEquals((NODE_SIZE + 2).toLong, all_rels(REL_SIZE + 1).startId)
    Assert.assertEquals(2.toLong, all_rels(REL_SIZE + 1).endId)
  }

  @Test
  def testCreateIndex(): Unit = {
    runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000}), (m:place {name: 'heaven'}), (n)-[r:livesIn]->(m) return n,r,m")
    runOnDemoGraph("CREATE INDEX ON :person(name)")
    runOnDemoGraph("CREATE INDEX ON :person(name, age)")
  }

}
