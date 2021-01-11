import org.grapheco.lynx._
import org.junit.{Assert, Test}

class CypherQueryTest extends TestBase {
  @Test
  def testQueryUnit(): Unit = {
    var rs: CypherResult = null
    rs = runOnDemoGraph("return 1")
    Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(CypherValue(1), rs.records().toSeq.apply(0)("1"))

    rs = runOnDemoGraph("return 1,2,3")
    Assert.assertEquals(Seq("1", "2", "3"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(CypherValue(1), rs.records().toSeq.apply(0)("1"))
    Assert.assertEquals(CypherValue(2), rs.records().toSeq.apply(0)("2"))
    Assert.assertEquals(CypherValue(3), rs.records().toSeq.apply(0)("3"))

    rs = runOnDemoGraph("return 1+2")
    Assert.assertEquals(Seq("1+2"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(CypherValue(3), rs.records().toSeq.apply(0)("1+2"))
  }

  @Test
  def testQueryUnitAsN(): Unit = {
    val rs = runOnDemoGraph("return 1 as N")
    Assert.assertEquals(Map("N" -> CypherValue(1)), rs.records.toSeq.apply(0))
    Assert.assertEquals(CypherValue(1), rs.records.toSeq.apply(0)("N"))
  }

  @Test
  def testQueryWithUnit(): Unit = {
    val rs = runOnDemoGraph("with 1 as N return N")
    Assert.assertEquals(Map("N" -> CypherValue(1)), rs.records.toSeq.apply(0))
    Assert.assertEquals(CypherValue(1), rs.records.toSeq.apply(0)("N"))
  }

  @Test
  def testQueryNodes(): Unit = {
    val rs = runOnDemoGraph("match (n) return n")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq(1.toLong, 2.toLong, 3.toLong), rs.records.toSeq.map(_.apply("n").asInstanceOf[CypherNode].id.value).toSeq)

  }

  @Test
  def testQueryNamedRelations(): Unit = {
    var rs = runOnDemoGraph("match (m)-[r]-(n) return m,r,n")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match (m)-[r]->(n) return m,r,n")
    Assert.assertEquals(3, rs.records.size)
    rs.records().foreach {
      map =>
        Assert.assertEquals(map("r").asInstanceOf[CypherRelationship].startNodeId, map("m").asInstanceOf[CypherNode].id)
        Assert.assertEquals(map("r").asInstanceOf[CypherRelationship].endNodeId, map("n").asInstanceOf[CypherNode].id)
    }

    rs = runOnDemoGraph("match (m)<-[r]-(n) return m,r,n")
    Assert.assertEquals(3, rs.records.size)
    rs.records().foreach {
      map =>
        Assert.assertEquals(map("r").asInstanceOf[CypherRelationship].startNodeId, map("n").asInstanceOf[CypherNode].id)
        Assert.assertEquals(map("r").asInstanceOf[CypherRelationship].endNodeId, map("m").asInstanceOf[CypherNode].id)
    }
  }

  @Test
  def testQueryAnonymousRelations(): Unit = {
    var rs = runOnDemoGraph("match ()-[r]-() return r")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match ()-[r]->() return r")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match ()<-[r]-() return r")
    Assert.assertEquals(3, rs.records.size)
  }

  @Test
  def testQueryDistinctRelations(): Unit = {
    val rs = runOnDemoGraph("match (n)-[r]-(m) return distinct r")
    Assert.assertEquals(3, rs.records.size)
  }

  @Test
  def testQueryMRN(): Unit = {
    val rs = runOnDemoGraph("match (m)-[r]->(n) return m,r,n")
    Assert.assertEquals(3, rs.records.size)

    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[CypherRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)

    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[CypherRelationship].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[CypherNode].id.value)
  }

  @Test
  def testQueryNodeProperty(): Unit = {
    var rs = runOnDemoGraph("match (n) return 1,1+2,2>1,n,n.name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq("bluejoe", "alex", "simba"),
      rs.records().toSeq.map(_.apply("n.name").asInstanceOf[CypherString].value).toSeq)
    Assert.assertEquals(CypherValue("bluejoe"), rs.records.toSeq.apply(0).apply("n.name"))
    Assert.assertEquals(CypherValue(1), rs.records().toSeq.apply(0)("1"))
    Assert.assertEquals(CypherValue(3), rs.records().toSeq.apply(0)("1+2"))
    Assert.assertEquals(CypherValue(true), rs.records().toSeq.apply(0)("2>1"))

    rs = runOnDemoGraph("match (n) return 1,1+2,2>1 as v0,n,n.name as name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq("bluejoe", "alex", "simba"),
      rs.records().toSeq.map(_.apply("name").asInstanceOf[CypherString].value).toSeq)
    Assert.assertEquals(CypherValue("bluejoe"), rs.records.toSeq.apply(0).apply("name"))
    Assert.assertEquals(CypherValue(true), rs.records().toSeq.apply(0)("v0"))
  }

  @Test
  def testQueryNodePropertyAlias(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.name as name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(CypherValue("bluejoe"), rs.records.toSeq.apply(0).apply("name"))
  }

  @Test
  def testQueryNodesWithFilter(): Unit = {
    val rs = runOnDemoGraph("match (n) where n.name='bluejoe' return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
  }

  @Test
  def testQueryNodeWithLabels(): Unit = {
    var rs = runOnDemoGraph("match (n:person) return n")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[CypherNode].id.value)

    rs = runOnDemoGraph("match (n:leader) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)

    rs = runOnDemoGraph("match (n:person:leader) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)

    rs = runOnDemoGraph("match (n:nonexisting) return n")
    Assert.assertEquals(0, rs.records.size)
  }

  @Test
  def testQueryPathWithRelationType(): Unit = {
    var rs = runOnDemoGraph("match ()-[r:knows]-() return r")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match ()-[r:nonexisting]-() return r")
    Assert.assertEquals(0, rs.records.size)

    rs = runOnDemoGraph("match (n)-[r:knows]-(m) return n,r,m")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[CypherNode].id.value)

    rs = runOnDemoGraph("match (n:person)-[r:knows]-(m) return n,r,m")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[CypherNode].id.value)
  }

  @Test
  def testQueryPathWithNodeLabel(): Unit = {
    var rs = runOnDemoGraph("match (n:person)-[r]->(m) return n")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)

    rs = runOnDemoGraph("match (n:person)-[r]->(m:person) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[CypherNode].id.value)
  }
}
