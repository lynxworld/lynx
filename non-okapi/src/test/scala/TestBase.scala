import org.grapheco.lynx.{CypherId, CypherNode, CypherRelationship, CypherResult, CypherRunner, CypherValue, GraphModel, Node2Create, Relationship2Create}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{LabelName, SemanticDirection}

import scala.collection.mutable.ArrayBuffer

class TestBase {
  Profiler.enableTiming = true

  val node1 = TestNode(1, Seq("person", "leader"), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
  val node2 = TestNode(2, Seq("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(30))
  val node3 = TestNode(3, Seq(), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, Some("knows")),
    TestRelationship(2, 2, 3, Some("knows")),
    TestRelationship(3, 1, 3, None)
  )

  val runner = new CypherRunner(new GraphModel {
    override def createElements(nodes: Array[Node2Create], rels: Array[Relationship2Create]): Unit = ???

    override def nodeAt(id: CypherId): Option[CypherNode] = {
      nodeAt(id.value.asInstanceOf[Long])
    }

    def nodeAt(id: Long): Option[CypherNode] = {
      all_nodes.find(_.id0 == id)
    }

    override def nodes(): Iterator[CypherNode] = all_nodes.iterator

    override def rels(
                       includeStartNodes: Boolean,
                       includeEndNodes: Boolean
                     ): Iterator[(CypherRelationship, Option[CypherNode], Option[CypherNode])] = {
      all_rels.map(rel =>
        (rel,
          if (includeStartNodes) {
            nodeAt(rel.startId)
          }
          else {
            None
          },
          if (includeEndNodes) {
            nodeAt(rel.endId)
          }
          else {
            None
          })
      ).iterator
    }
  })

  def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): CypherResult = {
    println(s"query: $query")
    runner.compile(query)
    Profiler.timing {
      val rs = runner.cypher(query, param)
      rs.show()
      rs
    }
  }

}

case class TestCypherId(value: Long) extends CypherId {
}

case class TestNode(id0: Long, labels: Seq[String], props: (String, CypherValue)*) extends CypherNode {
  lazy val properties = props.toMap
  override val id: CypherId = TestCypherId(id0)

  override def property(name: String): Option[CypherValue] = properties.get(name)
}

case class TestRelationship(id0: Long, startId: Long, endId: Long, relationType: Option[String], props: (String, CypherValue)*) extends CypherRelationship {
  lazy val properties = props.toMap
  override val id: CypherId = TestCypherId(id0)
  override val startNodeId: CypherId = TestCypherId(startId)
  override val endNodeId: CypherId = TestCypherId(endId)

  override def property(name: String): Option[CypherValue] = properties.get(name)
}
