package org.grapheco.lynx

import org.junit.{Assert, Test}

class CypherQueryTest extends TestBase {
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
  def testQueryMultiplePaths(): Unit = {
    var rs = runOnDemoGraph("match ()-[r]-(n),(n)-[s]-() return r,s")
    Assert.assertEquals(6, rs.records.size)

    rs = runOnDemoGraph("match ()-[r]->(n),(n)-[s]-() return r,s")
    Assert.assertEquals(3, rs.records.size)

    rs = runOnDemoGraph("match (m)-[r]->(n),(n)-[s]->(x) return r,s")
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
  def testQueryPathWithRelationType(): Unit = {
    var rs = runOnDemoGraph("match ()-[r:KNOWS]-() return r")
    Assert.assertEquals(4, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(2).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(3).apply("r").asInstanceOf[LynxRelationship].id.value)

    rs = runOnDemoGraph("match ()-[r:NON_EXISTING]-() return r")
    Assert.assertEquals(0, rs.records.size)

    rs = runOnDemoGraph("match (n)-[r:KNOWS]-(m) return n,r,m")
    Assert.assertEquals(4, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(2).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(3).apply("r").asInstanceOf[LynxRelationship].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(2).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(2).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(3).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(3).apply("m").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:person)-[r:KNOWS]->(m) return n,r,m")
    Assert.assertEquals(2, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(1).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(3.toLong, rs.records.toSeq.apply(1).apply("m").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n)-[r:KNOWS]->(m:person) return n,r,m")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)

    rs = runOnDemoGraph("match (n:leader)-[r:KNOWS]->(m) return n,r,m")
    Assert.assertEquals(1, rs.records.size)
    Assert.assertEquals(1.toLong, rs.records.toSeq.apply(0).apply("n").asInstanceOf[LynxNode].id.value)
    Assert.assertEquals(2.toLong, rs.records.toSeq.apply(0).apply("m").asInstanceOf[LynxNode].id.value)
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

  @Test
  def testQueryOrderBy(): Unit = {
    var rs1 = runOnDemoGraph("create (n1:test{age:10,name:5}),(n2:test{age:10,name:4}),(n3:test{age:11,name:3})")
    var rs2 = runOnDemoGraph("match (n:test) return n.age,n.name  order by n.age desc, n.name")
    var rs3 = runOnDemoGraph("match (n) return  count(n.name),count(n.age),count(n.nonexist),count(1),count(1+1)")
  }

  @Test
  def testQueryCaseWhen(): Unit = {
    var rs1 = runOnDemoGraph("create (n1:test{age:9,name:'CodeBaby'}),(n2:test{age:10,name:'BlueJoy'}),(n3:test{age:18,name:'OldWang'})")
    var rs = runOnDemoGraph("match (n:test) WITH n.age as age, CASE WHEN n.age < 10 THEN 'Child' WHEN n.age <16 THEN 'Teenager' ELSE 'Adult' END AS hood,n.name as name RETURN age,name,hood order by age desc,name")
  }

  @Test
  def testFunction(): Unit ={
    runOnDemoGraph("return lynx()")
    runOnDemoGraph("return toInterger('345')")
  }

  @Test
  def testFunction23(): Unit ={
    runOnDemoGraph("return 2,toInterger('345'), date('2018-05-06')")
  }

  @Test
  def testmatch(): Unit ={
    runOnDemoGraph("match (n{name:'alex'}) return n")
  }

  @Test
  def testmatchxing(): Unit ={
    runOnDemoGraph("match data =(:leader)-[:KNOWS*3..2]->() return data")
  }

  @Test
  def testmatchxing2(): Unit ={
    runOnDemoGraph("match (n:leader)-[:KNOWS*3..2]->() return n")
  }
}
