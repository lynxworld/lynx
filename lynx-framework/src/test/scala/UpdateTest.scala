import org.junit.{Assert, Test}
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherString}

class UpdateTest extends MyTest {
  @Test
  def testCreateNode(): Unit = {
    val size1 = nodes.size
    val size2 = rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'bluejoe', age: 40})")
    rs.show
    Assert.assertEquals(size1 + 1, nodes.size)
    Assert.assertEquals(size2, rels.size)
  }

  @Test
  def testCreateNodeWithReturn(): Unit = {
    val size1 = nodes.size
    val size2 = rels.size
    val rs = runOnDemoGraph("CREATE (n {name: 'bluejoe', age: 40}) return n")
    rs.show
    Assert.assertEquals(size1 + 1, nodes.size)
    Assert.assertEquals(size2, rels.size)
  }

  @Test
  def testCreateNodesRelation(): Unit = {
    val size1 = nodes.size
    val size2 = rels.size
    val rs = runOnDemoGraph("CREATE (n:person {name: 'bluejoe', age: 40}),(m:person {name: 'alex', age: 30}),(n)-[:knows]->(m)")
    rs.show
    Assert.assertEquals(size1 + 2, nodes.size)
    Assert.assertEquals(size2 + 1, rels.size)

    Assert.assertEquals(CypherString("bluejoe"), nodes(size1).properties("name"))
    Assert.assertEquals(CypherInteger(40), nodes(size1).properties("age"))
    Assert.assertEquals(Set("person"), nodes(size1).labels)

    Assert.assertEquals(CypherString("alex"), nodes(size1 + 1).properties("name"))
    Assert.assertEquals(CypherInteger(30), nodes(size1 + 1).properties("age"))
    Assert.assertEquals(Set("person"), nodes(size1 + 1).labels)

    Assert.assertEquals("knows", rels(size2).relType)
    Assert.assertEquals(nodes(size1 + 1).id, rels(size2).startId)
  }
}
