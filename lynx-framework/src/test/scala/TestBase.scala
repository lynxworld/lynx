import org.opencypher.lynx.ir.{IRContextualNodeRef, IRNode, IRNodeRef, IRRelation, IRStoredNodeRef, PropertyGraphWriter}
import org.opencypher.lynx.{LynxSession, PropertyGraphScanner}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTInteger, CTString, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, Relationship}
import org.opencypher.okapi.impl.schema.PropertyGraphSchemaImpl

import scala.collection.mutable.ArrayBuffer

class TestBase {
  val _session = new LynxSession()

  val node1 = TestNode(1, Set("person", "t1"), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
  val node2 = TestNode(2, Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(30))
  val node3 = TestNode(3, Set(), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
  val nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val rels = ArrayBuffer[TestRelationship](TestRelationship(1, 1, 2, "knows"), TestRelationship(2, 2, 3, "knows"))
  val graphDemo = _session.createPropertyGraph(new PropertyGraphScanner[Long] {

    override def allNodes(): Iterable[Node[Long]] = nodes

    override def allRelationships(): Iterable[Relationship[Long]] = rels

    override def schema: PropertyGraphSchema =
      PropertyGraphSchemaImpl(
        Map[Set[String], Map[String, CypherType]](
          Set[String]("person", "t1") -> Map("name" -> CTString, "age" -> CTInteger)
        ),
        Map[String, Map[String, CypherType]](
          "knows" -> Map[String, CypherType]()
        )
      )

    override def nodeAt(id: Long): Node[Long] = nodes.find(_.id == id).get
  }, new PropertyGraphWriter[Long] {
    override def createElements(nodes1: Array[IRNode], rels1: Array[IRRelation[Long]]): Unit = {
      val nodesMap = nodes1.map(node => {
        val id = nodes.size + 1
        node -> id.toLong
      }).toMap

      def nodeId(ref: IRNodeRef[Long]) = {
        ref match {
          case IRStoredNodeRef(id) => id
          case IRContextualNodeRef(node) => nodesMap(node)
        }
      }

      nodes ++= nodesMap.map({
        case (node, id) =>
          TestNode(id, node.labels.toSet, node.props: _*)
      })

      rels ++= rels1.map(rel => TestRelationship(rels.size + 1, startId = nodeId(rel.startNodeRef), endId = nodeId(rel.endNodeRef), rel.types.head, rel.props: _*))
    }
  })
}

case class TestNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] {
  lazy val properties = props.toMap
  val withIds = props.toMap + ("_id" -> CypherValue(id))
  override type I = this.type

  override def copy(id: Long, labels: Set[String], properties: CypherMap): TestNode.this.type = this
}

case class TestRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends Relationship[Long] {
  val properties = props.toMap
  val withIds = props.toMap ++ Map("_id" -> CypherValue(id), "_from" -> CypherValue(startId), "_to" -> CypherValue(endId))
  override type I = this.type

  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): TestRelationship.this.type = this
}
