import org.grapheco.lynx.{LynxInteger, LynxNode, LynxString, LynxValue}
import org.junit.{Assert, Test}

class CypherCreateTest extends TestBase {
  @Test
  def testCreateNode(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000})")
    Assert.assertEquals(size1 + 1, all_nodes.size)
    Assert.assertEquals(size2, all_rels.size)
    Assert.assertEquals(LynxValue("God"), all_nodes.apply(size1).property("name").get)
  }

  @Test
  def testCreateNodeWithReturn(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'God', age: 10000}) return n")
    Assert.assertEquals(size1 + 1, all_nodes.size)
    Assert.assertEquals(size2, all_rels.size)
    Assert.assertEquals((size1 + 1).toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testCreateNodesRelation(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n:person {name: 'God', age: 10000}), (m:place {name: 'heaven'}), (n)-[r:livesIn]->(m) return n,r,m")
    Assert.assertEquals(size1 + 2, all_nodes.size)
    Assert.assertEquals(size2 + 1, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(size1).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(size1).properties("age"))
    Assert.assertEquals(Seq("person"), all_nodes(size1).labels)

    Assert.assertEquals(LynxString("heaven"), all_nodes(size1 + 1).properties("name"))
    Assert.assertEquals(Seq("place"), all_nodes(size1 + 1).labels)

    Assert.assertEquals("livesIn", all_rels(size2).relationType.get)
    Assert.assertEquals(all_nodes(size1 + 1).id.value, all_rels(size2).startId)
    Assert.assertEquals(all_nodes(size1 + 1).id.value, all_rels(size2).endId)
  }

  @Test
  def testCreateNodesPath(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (a:person {name: 'BaoChai'}), (b:person {name: 'BaoYu'}), (c:person {name: 'DaiYu'}), (a)-[:LOVES]->(b)-[:LOVES]->(c) return a,b,c")
    Assert.assertEquals(size1 + 2, all_nodes.size)
    Assert.assertEquals(size2 + 2, all_rels.size)

    Assert.assertEquals(LynxString("BaoChai"), all_nodes(size1).properties("name"))
    Assert.assertEquals(LynxString("BaoYu"), all_nodes(size1 + 1).properties("name"))
    Assert.assertEquals(LynxString("DaiYu"), all_nodes(size1 + 2).properties("name"))

    Assert.assertEquals("LOVES", all_rels(size2).relationType.get)
    Assert.assertEquals(all_nodes(size1).id.value, all_rels(size2).startId)
    Assert.assertEquals(all_nodes(size1 + 1).id.value, all_rels(size2).endId)

    Assert.assertEquals("LOVES", all_rels(size2 + 1).relationType.get)
    Assert.assertEquals(all_nodes(size1 + 1).id.value, all_rels(size2 + 1).startId)
    Assert.assertEquals(all_nodes(size1 + 2).id.value, all_rels(size2 + 1).endId)
  }

  @Test
  def testMatchAndCreateRelation(): Unit = {
     var rs = runOnDemoGraph("match (m:person {name:'bluejoe'}) (n {name:'CNIC'}) CREATE (m)-[r:WORKS_FOR]->(n) return m,r,n")
    Assert.assertEquals(3, all_nodes.size)
    Assert.assertEquals(4, all_rels.size)

    Assert.assertEquals("WORKS_FOR", all_rels(3).relationType.get)
    Assert.assertEquals(1.toLong, all_rels(3).startId)
    Assert.assertEquals(3.toLong, all_rels(3).endId)
  }

  @Test
  def testMatchToCreateNodesAndRelations(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    var rs = runOnDemoGraph("match (m:person) CREATE (n {name: 'God', age: 10000}), (n)-[r:LOVES]->(m) return n,r,m")
    Assert.assertEquals(size1 + 1, all_nodes.size)
    Assert.assertEquals(size2 + 2, all_rels.size)

    Assert.assertEquals(LynxString("God"), all_nodes(size1).properties("name"))
    Assert.assertEquals(LynxInteger(10000), all_nodes(size1).properties("age"))
    Assert.assertEquals(Seq("person"), all_nodes(size1).labels)

    Assert.assertEquals("LOVES", all_rels(size2).relationType.get)
    Assert.assertEquals(size1.toLong, all_rels(size2).startId)
    Assert.assertEquals(1.toLong, all_rels(size2).endId)
    Assert.assertEquals("LOVES", all_rels(size2 + 1).relationType.get)
    Assert.assertEquals(size1.toLong, all_rels(size2 + 1).startId)
    Assert.assertEquals(2.toLong, all_rels(size2 + 1).endId)
  }
}
