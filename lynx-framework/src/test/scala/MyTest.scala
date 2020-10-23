import org.junit.{Assert, Test}
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.{LynxDataFrame, LynxRecords, LynxSession, RecordHeader}
import org.opencypher.okapi.api.graph.{SourceEndNodeKey, SourceStartNodeKey}
import org.opencypher.okapi.api.schema.{LabelPropertyMap, PropertyGraphSchema, PropertyKeys, RelTypePropertyMap}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CTString, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherString, CypherValue, Node, Relationship}
import org.opencypher.okapi.impl.schema.PropertyGraphSchemaImpl
import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}

case class LynxNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] {
  lazy val properties = props.toMap
  val withIds = props.toMap + ("_id" -> CypherValue(id))
  override type I = this.type

  override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode.this.type = this
}

case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends Relationship[Long] {
  val properties = props.toMap
  val withIds = props.toMap ++ Map("_id" -> CypherValue(id), "_from" -> CypherValue(startId), "_to" -> CypherValue(endId))
  override type I = this.type

  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): LynxRelationship.this.type = this
}

class MyTest {
  val _session = new LynxSession()

  val graphDemo = new LynxPropertyGraph() {
    override implicit def session: LynxSession = _session

    val node1 = LynxNode(1, Set(), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
    val node2 = LynxNode(2, Set(), "name" -> CypherValue("alex"), "age" -> CypherValue(30))
    val node3 = LynxNode(3, Set(), "name" -> CypherValue("simba"), "age" -> CypherValue(10))

    override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords = {
      LynxRecords(
        RecordHeader(Map(NodeVar(name)(CTNode) -> name)),
        LynxDataFrame(
          Map(name -> CTNode),
          Array(
            Map(name -> node1),
            Map(name -> node2),
            Map(name -> node3)
          ).toStream)
      )
    }

    override def relationships(name: String, relCypherType: CTRelationship): LynxRecords = {
      LynxRecords(
        RecordHeader(Map(
          RelationshipVar(name)(CTRelationship) -> name,
          StartNode(RelationshipVar(name)(CTRelationship))(CTNode) -> SourceStartNodeKey.name,
          EndNode(RelationshipVar(name)(CTRelationship))(CTNode) -> SourceEndNodeKey.name
        )),
        LynxDataFrame(
          Map(name -> CTRelationship, SourceStartNodeKey.name -> CTNode, SourceEndNodeKey.name -> CTNode),
          Array(
            Map(name -> LynxRelationship(1, 1, 2, "knows"), SourceStartNodeKey.name -> node1, SourceEndNodeKey.name -> node2),
            Map(name -> LynxRelationship(2, 2, 3, "knows"), SourceStartNodeKey.name -> node2, SourceEndNodeKey.name -> node3)
          ).toStream)
      )
    }

    override def schema: PropertyGraphSchema =
      PropertyGraphSchemaImpl(
        Map[Set[String], Map[String, CypherType]](
          Set[String]() -> Map("name" -> CTString)
        ),
        Map[String, Map[String, CypherType]](
          "knows" -> Map[String, CypherType]()
        )
      )
  }

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
    val rs = runOnDemoGraph("match (n)-[r]-(m) return r")
    Assert.assertEquals(2, rs.collect.size)
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

  private def runOnEmptyGraph(query: String): CypherRecords = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = _session.cypher(query).records
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    records.show
    records
  }

  private def runOnDemoGraph(query: String): CypherRecords = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = graphDemo.cypher(query)
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    records.show
    records.records
  }

}
