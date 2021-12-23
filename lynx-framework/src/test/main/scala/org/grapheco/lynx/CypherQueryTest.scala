package org.grapheco.lynx

import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

class CypherQueryTest extends TestBase {

  val n1 = TestNode(1, Seq("person", "leader"), "name" -> LynxValue("bluejoe"), "age" -> LynxValue(40))
  val n2 = TestNode(2, Seq("person"), "name" -> LynxValue("alex"), "age" -> LynxValue(30))
  val n3 = TestNode(3, Seq(), "name" -> LynxValue("CNIC"), "age" -> LynxValue(10))

  val r1 = TestRelationship(1, 1, 2, Some("knows"))
  val r2 = TestRelationship(2, 2, 3, Some("knows"))
  val r3 = TestRelationship(3, 1, 3, None)

  all_nodes.clear()
  all_nodes.append(n1, n2, n3)
  all_rels.clear()
  all_rels.append(r1, r2, r3)

  @Test
  def testQueryUnit(): Unit = {
    var rs: LynxResult = null
    rs = runOnDemoGraph("return 1")
    Assert.assertEquals(Seq("1"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(LynxValue(1), rs.records().toSeq.apply(0)("1"))

    rs = runOnDemoGraph("return 1,2,3")
    Assert.assertEquals(Seq("1", "2", "3"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(LynxValue(1), rs.records().toSeq.apply(0)("1"))
    Assert.assertEquals(LynxValue(2), rs.records().toSeq.apply(0)("2"))
    Assert.assertEquals(LynxValue(3), rs.records().toSeq.apply(0)("3"))

    rs = runOnDemoGraph("return 1+2")
    Assert.assertEquals(Seq("1+2"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(LynxValue(3), rs.records().toSeq.apply(0)("1+2"))
  }

  @Test
  def testQueryUnitWithParams(): Unit = {
    var rs: LynxResult = null
    rs = runOnDemoGraph("return $N", Map("N" -> 1))
    Assert.assertEquals(Seq("$N"), rs.columns)
    Assert.assertEquals(1, rs.records().size)
    Assert.assertEquals(LynxValue(1), rs.records().toSeq.apply(0)("$N"))
  }

  @Test
  def testQueryUnitAsN(): Unit = {
    val rs = runOnDemoGraph("return 1 as N")
    Assert.assertEquals(Map("N" -> LynxValue(1)), rs.records.toSeq.apply(0))
    Assert.assertEquals(LynxValue(1), rs.records.toSeq.apply(0)("N"))
  }

  @Test
  def testQueryWithUnit(): Unit = {
    val rs = runOnDemoGraph("with 1 as N return N")
    Assert.assertEquals(Map("N" -> LynxValue(1)), rs.records.toSeq.apply(0))
    Assert.assertEquals(LynxValue(1), rs.records.toSeq.apply(0)("N"))
  }

  @Test
  def testQueryNodes(): Unit = {
    val rs = runOnDemoGraph("match (n) return n")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq(1.toLong, 2.toLong, 3.toLong), rs.records.toSeq.map(_.apply("n").asInstanceOf[LynxNode].id.value).toSeq)

  }

  @Test
  def testQueryWithLimit(): Unit = {
    var rs = runOnDemoGraph("match (n) return n limit 2")
    Assert.assertEquals(2, rs.records.size)

    rs = runOnDemoGraph("match (n) return n limit 1")
    Assert.assertEquals(1, rs.records.size)

    rs = runOnDemoGraph("match (n) return n limit 3")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match (n) return n limit 10")
    Assert.assertEquals(3, rs.records.size)
  }

  @Test
  def testMatchWithReturn(): Unit = {
    val rs = runOnDemoGraph("match (n) with n.name as x, n.age as y return x,y")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(LynxValue("bluejoe"), rs.records.toSeq.apply(0).apply("x"))
    Assert.assertEquals(LynxValue(40), rs.records.toSeq.apply(0).apply("y"))
    Assert.assertEquals(LynxValue("alex"), rs.records.toSeq.apply(1).apply("x"))
    Assert.assertEquals(LynxValue(30), rs.records.toSeq.apply(1).apply("y"))
    Assert.assertEquals(LynxValue("CNIC"), rs.records.toSeq.apply(2).apply("x"))
    Assert.assertEquals(LynxValue(10), rs.records.toSeq.apply(2).apply("y"))
  }

  @Test
  def testMatchWithReturnEval(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.name,n.age+1")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(LynxValue(41), rs.records.toSeq.apply(0).apply("n.age+1"))
    Assert.assertEquals(LynxValue(31), rs.records.toSeq.apply(1).apply("n.age+1"))
    Assert.assertEquals(LynxValue(11), rs.records.toSeq.apply(2).apply("n.age+1"))
  }

  @Test
  def testMatchWhereWithReturn(): Unit = {
    var rs = runOnDemoGraph("match (n) where n.age>10 with n.name as x, n.age as y return x,y")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(LynxValue("bluejoe"), rs.records.toSeq.apply(0).apply("x"))
    Assert.assertEquals(LynxValue(40), rs.records.toSeq.apply(0).apply("y"))
    Assert.assertEquals(LynxValue("alex"), rs.records.toSeq.apply(1).apply("x"))
    Assert.assertEquals(LynxValue(30), rs.records.toSeq.apply(1).apply("y"))

    rs = runOnDemoGraph("match (n) where n.age>10 with n.name as x, n.age as y where y<40 return x,y")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(LynxValue("alex"), rs.records.toSeq.apply(0).apply("x"))
    Assert.assertEquals(LynxValue(30), rs.records.toSeq.apply(0).apply("y"))
  }

  @Test
  def testQueryNamedRelations(): Unit = {
    var rs = runOnDemoGraph("match (m)-[r]-(n) return m,r,n")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match (m)-[r]->(n) return m,r,n")
    Assert.assertEquals(3, rs.records.size)
    rs.records().foreach {
      map =>
        Assert.assertEquals(map("r").asInstanceOf[LynxRelationship].startNodeId, map("m").asInstanceOf[LynxNode].id)
        Assert.assertEquals(map("r").asInstanceOf[LynxRelationship].endNodeId, map("n").asInstanceOf[LynxNode].id)
    }

    rs = runOnDemoGraph("match (m)<-[r]-(n) return m,r,n")
    Assert.assertEquals(3, rs.records.size)
    rs.records().foreach {
      map =>
        Assert.assertEquals(map("r").asInstanceOf[LynxRelationship].startNodeId, map("n").asInstanceOf[LynxNode].id)
        Assert.assertEquals(map("r").asInstanceOf[LynxRelationship].endNodeId, map("m").asInstanceOf[LynxNode].id)
    }

    rs = runOnDemoGraph("match (m)<-[r]->(n) return m,r,n")
    Assert.assertEquals(6, rs.records.size)
  }

  @Test
  def testQueryPathTriple(): Unit = {
    var rs = runOnDemoGraph("match ()-[r]-() return r")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match ()-[r]->() return r")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match ()<-[r]-() return r")
    Assert.assertEquals(3, rs.records.size)
  }

  @Test
  def testQuerySingleLongPath(): Unit = {
    var rs = runOnDemoGraph("match ()-[r]-()-[s]-() return r,s")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match ()-[r]->()-[s]-() return r,s")
    Assert.assertEquals(3, rs.records.size)

    //(bluejoe)-[:KNOWS]->(alex)-[:KNOWS]->(CNIC)
    rs = runOnDemoGraph("match (m)-[r]->(n)-[s]->(x) return r,s")
    Assert.assertEquals(1, rs.records.size)

    //(bluejoe)-[:KNOWS]->(alex)-[:KNOWS]->(CNIC)
    rs = runOnDemoGraph("match (m)-[r]->(n)-[s]->(x)<-[]-(m) return r,s")
    Assert.assertEquals(1, rs.records.size)
  }

  @Test
  def testQueryMultipleMatchs(): Unit = {
    var rs = runOnDemoGraph("match ()-[r]-(n) match (n)-[s]-() return r,s")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match ()-[r]->(n) match (n)-[s]-() return r,s")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match (m)-[r]->(n) match (n)-[s]->(x) return r,s")
    Assert.assertEquals(1, rs.records.size)

    rs = runOnDemoGraph("match (m)-[r]->(n) where m.age>18 match (n)-[s]->(x) where x.age<35 return r,s")
    Assert.assertEquals(1, rs.records.size)
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

    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testQueryNodeProperty(): Unit = {
    var rs = runOnDemoGraph("match (n) return 1,1+2,2>1,n,n.name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq("bluejoe", "alex", "CNIC"),
      rs.records().toSeq.map(_.apply("n.name").asInstanceOf[LynxString].value).toSeq)
    Assert.assertEquals(LynxValue("bluejoe"), rs.records.toSeq.apply(0).apply("n.name"))
    Assert.assertEquals(LynxValue(1), rs.records().toSeq.apply(0)("1"))
    Assert.assertEquals(LynxValue(3), rs.records().toSeq.apply(0)("1+2"))
    Assert.assertEquals(LynxValue(true), rs.records().toSeq.apply(0)("2>1"))

    rs = runOnDemoGraph("match (n) return 1,1+2,2>1 as v0,n,n.name as name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(Seq("bluejoe", "alex", "CNIC"),
      rs.records().toSeq.map(_.apply("name").asInstanceOf[LynxString].value).toSeq)
    Assert.assertEquals(LynxValue("bluejoe"), rs.records.toSeq.apply(0).apply("name"))
    Assert.assertEquals(LynxValue(true), rs.records().toSeq.apply(0)("v0"))
  }

  @Test
  def testQueryNodePropertyAlias(): Unit = {
    val rs = runOnDemoGraph("match (n) return n.name as name")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(LynxValue("bluejoe"), rs.records.toSeq.apply(0).apply("name"))
  }

  @Test
  def testQueryNodesWithFilter(): Unit = {
    var rs = runOnDemoGraph("match (n) where n.name='bluejoe' return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n) where n.name=$name return n", Map("name" -> "bluejoe"))
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testQueryNodeWithLabels(): Unit = {
    var rs = runOnDemoGraph("match (n:person) return n")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:leader) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:person:leader) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:nonexisting) return n")
    Assert.assertEquals(0, rs.records.size)
  }

  @Test
  def testQueryNodeWithProperties(): Unit = {
    var rs = runOnDemoGraph("match (n {name: 'bluejoe'}) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n {name: 'CNIC'}) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n {name: 'nonexisting'}) return n")
    Assert.assertEquals(0, rs.records.size)

    rs = runOnDemoGraph("match (n:nonexisting {name: 'bluejoe'}) return n")
    Assert.assertEquals(0, rs.records.size)

    rs = runOnDemoGraph("match (n:leader {name: 'bluejoe'}) return n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testQueryPathWithNodeLabel(): Unit = {
    var rs = runOnDemoGraph("match (n:person)-[r]->(m) return n,r,m")
    Assert.assertEquals(3, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(2).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(2).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(2).apply("m").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:person)-[r]->(m:person) return n,r,m")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testQueryPathWithNodeProperties(): Unit = {
    var rs = runOnDemoGraph("match (n {name:'bluejoe'})-[r]->(m) return n,r,m")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:person {name:'bluejoe'})-[r]->(m:person) return n,r,m")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
  }

  @Test
  def testQueryPathMN(): Unit = {
    var rs = runOnDemoGraph("match (m {name:'bluejoe'})-->(n) return m,n")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (m:person)-->(n:person) return m,n")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (m:person)--(n:person) return m,n")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)

    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
  }
}
