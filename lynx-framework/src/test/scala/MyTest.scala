import org.junit.{Assert, Test}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString, CypherValue, Node, Relationship}

class MyTest extends TestBase {

  @Test
  def testEmptyGraph(): Unit = {
    var rs: CypherRecords = null
    rs = runOnEmptyGraph("return 1")
    Assert.assertEquals(Seq("1"), rs.physicalColumns)
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(CypherMap("1" -> 1), rs.collect.apply(0))

    rs = runOnEmptyGraph("return 1+2")
    Assert.assertEquals(Seq("1+2"), rs.physicalColumns)
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(CypherMap("1+2" -> 3), rs.collect.apply(0))

    rs = runOnEmptyGraph("return 1 AS x")
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(CypherMap("x" -> 1), rs.collect.apply(0))

    rs = runOnEmptyGraph("match (n) return n")
    Assert.assertEquals(0, rs.collect.size)
  }

  @Test
  def testQueryUnit(): Unit = {
    var rs: CypherRecords = null
    rs = runOnDemoGraph("return 1")
    Assert.assertEquals(Seq("1"), rs.physicalColumns)
    Assert.assertEquals(1, rs.collect.size)
    rs = runOnDemoGraph("return 1+2")
    Assert.assertEquals(Seq("1+2"), rs.physicalColumns)
    rs = runOnDemoGraph("match (n) with n as x return x.name")
  }

  @Test
  def testQueryUnitAsN(): Unit = {
    val rs = runOnDemoGraph("return 1 as N")
    Assert.assertEquals(CypherMap("N" -> 1), rs.collect.apply(0))
    Assert.assertEquals(CypherValue(1), rs.collect.apply(0).apply("N"))

  }

  @Test
  def testQueryNodes(): Unit = {
    val rs = runOnDemoGraph("match (n) return n")
    Assert.assertEquals(3, rs.collect.size)
    Assert.assertEquals(Seq(1, 2, 3), rs.collect.map(_.apply("n").cast[Node[Long]].id).toSeq)

  }

  @Test
  def testQueryRelations(): Unit = {
    val rs = runOnDemoGraph("match (m)-[r]-(n) return m,r,n")
    rs.show
    Assert.assertEquals(4, rs.collect.size)
  }

  @Test
  def testQueryDirectedRelations(): Unit = {
    val rs = runOnDemoGraph("match (n)-[r]->(m) return r")
    rs.show
    Assert.assertEquals(2, rs.collect.size)
  }

  @Test
  def testQueryDistinctRelations(): Unit = {
    val rs = runOnDemoGraph("match (n)-[r]-(m) return distinct r")
    rs.show
    Assert.assertEquals(2, rs.collect.size)
  }

  @Test
  def testQueryRelationsWithPath(): Unit = {
    val rs = runOnDemoGraph("match (n)-[r]-(m) return m,n,r")
    rs.show
    Assert.assertEquals(4, rs.collect.size)
  }

  @Test
  def testQueryMRN(): Unit = {
    val rs = runOnDemoGraph("match (m)-[r]->(n) return m,r,n")
    Assert.assertEquals(2, rs.collect.size)

    Assert.assertEquals(1, rs.collect.apply(0).apply("m").cast[Node[Long]].id)
    Assert.assertEquals(1, rs.collect.apply(0).apply("r").cast[Relationship[Long]].id)
    Assert.assertEquals(2, rs.collect.apply(0).apply("n").cast[Node[Long]].id)

    Assert.assertEquals(2, rs.collect.apply(1).apply("m").cast[Node[Long]].id)
    Assert.assertEquals(2, rs.collect.apply(1).apply("r").cast[Relationship[Long]].id)
    Assert.assertEquals(3, rs.collect.apply(1).apply("n").cast[Node[Long]].id)

  }

  @Test
  def testQueryNodeProperty(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.name")
    Assert.assertEquals(3, rs.collect.size)
    Assert.assertEquals(Seq("bluejoe", "alex", "simba"),
      rs.collect.map(_.apply("n.name").cast[CypherString].value).toSeq)
  }

  @Test
  def testQueryNodePropertyAlias(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.name as name")
    Assert.assertEquals(3, rs.collect.size)
    Assert.assertEquals(CypherValue("bluejoe"), rs.collect.apply(0).apply("name"))
  }

  @Test
  def testQueryNodesWithFilter(): Unit = {
    val rs = runOnDemoGraph("match (n) where n.name='bluejoe' return n")
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
  }

  @Test
  def testQueryWithLabels(): Unit = {
    var rs = runOnDemoGraph("match (n:person) return n")
    Assert.assertEquals(2, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
    Assert.assertEquals(2, rs.collect.apply(1).apply("n").cast[Node[Long]].id)

    rs = runOnDemoGraph("match (n:t1) return n")
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)

    rs = runOnDemoGraph("match (n:person:t1) return n")
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)

    rs = runOnDemoGraph("match (n:t2) return n")
    Assert.assertEquals(0, rs.collect.size)
  }

  @Test
  def testQueryWithLabelsAndRelation(): Unit = {
    val rs = runOnDemoGraph("match (n:person)-[r]->(m) return n")
    rs.show
    Assert.assertEquals(2, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
  }

  @Test
  def testQueryWithLabelsAndRelationWithLabel(): Unit = {
    val rs = runOnDemoGraph("match (n:person)-[r:knows]->(m) return n")
    rs.show
    Assert.assertEquals(2, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
  }

  @Test
  def testQueryWithRelationLabel(): Unit = {
    val rs = runOnDemoGraph("match (n:person)-[r:knows]-(m) return n,r,m")
    rs.show
    Assert.assertEquals(3, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
    Assert.assertEquals(2, rs.collect.apply(0).apply("m").cast[Node[Long]].id)
  }

  @Test
  def testQueryWithNodeLabel(): Unit = {
    val rs = runOnDemoGraph("match (n:person)-[r]->(m:person) return n")
    rs.show
    Assert.assertEquals(1, rs.collect.size)
    Assert.assertEquals(1, rs.collect.apply(0).apply("n").cast[Node[Long]].id)
  }

  protected def runOnEmptyGraph(query: String): CypherRecords = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = _session.cypher(query).records
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    records.show
    records
  }

  protected def runOnDemoGraph(query: String): CypherRecords = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = graphDemo.cypher(query)
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    //records.show
    records.records
  }
}
