import org.grapheco.lynx.{CypherInteger, CypherString}
import org.junit.{Assert, Test}

class CypherUpdateTest extends CypherQueryTest {
  @Test
  def testCreateNode(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'bluejoe', age: 40})")
    Assert.assertEquals(size1 + 1, all_nodes.size)
    Assert.assertEquals(size2, all_rels.size)
  }

  @Test
  def testCreateNodeWithReturn(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'bluejoe', age: 40}) return n")
    Assert.assertEquals(size1 + 1, all_nodes.size)
    Assert.assertEquals(size2, all_rels.size)
  }

  @Test
  def testCreateNodesRelation(): Unit = {
    val size1 = all_nodes.size
    val size2 = all_rels.size
    val rs = runOnDemoGraph("CREATE (n:person {name: 'bluejoe', age: 40}),(m:person {name: 'alex', age: 30}),(n)-[:knows]->(m)")
    Assert.assertEquals(size1 + 2, all_nodes.size)
    Assert.assertEquals(size2 + 1, all_rels.size)

    Assert.assertEquals(CypherString("bluejoe"), all_nodes(size1).properties("name"))
    Assert.assertEquals(CypherInteger(40), all_nodes(size1).properties("age"))
    Assert.assertEquals(Set("person"), all_nodes(size1).labels)

    Assert.assertEquals(CypherString("alex"), all_nodes(size1 + 1).properties("name"))
    Assert.assertEquals(CypherInteger(30), all_nodes(size1 + 1).properties("age"))
    Assert.assertEquals(Set("person"), all_nodes(size1 + 1).labels)

    Assert.assertEquals("knows", all_rels(size2).relType)
    Assert.assertEquals(all_nodes(size1 + 1).id, all_rels(size2).startId)
  }
}
