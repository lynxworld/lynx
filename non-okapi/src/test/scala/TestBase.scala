import org.grapheco.lynx.{CypherId, CypherNode, CypherRelationship, CypherResult, CypherRunner, CypherValue, GraphModel, IRNode, IRRelation}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{LabelName, SemanticDirection}

import scala.collection.mutable.ArrayBuffer

class TestBase {
  Profiler.enableTiming = true

  val node1 = TestNode(1, Set("person", "t1"), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
  val node2 = TestNode(2, Set("person"), "name" -> CypherValue("alex"), "age" -> CypherValue(30))
  val node3 = TestNode(3, Set(), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, "knows"),
    TestRelationship(2, 2, 3, "knows"),
    TestRelationship(3, 1, 3, "like")
  )

  private def nodeAt(id: Long): TestNode = {
    all_nodes.find(_.id0 == id).get
  }

  val runner = new CypherRunner(new GraphModel {
    override def createElements(nodes: Array[IRNode], rels: Array[IRRelation]): Unit = ???

    override def nodes(labels: Seq[String]): Iterator[CypherNode] = labels match {
      case Seq() =>
        all_nodes.iterator
      case _ =>
        all_nodes.filter(node => {
          true //(labels -- node.labels).isEmpty
        }).iterator
    }

    override def rels(types: Seq[String], labels1: Seq[LabelName], labels2: Seq[LabelName],
                      includeStartNodes: Boolean,
                      includeEndNodes: Boolean
                     ): Iterator[(CypherRelationship, Option[CypherNode], Option[CypherNode])] = {
      (labels1, labels2) match {
        case (Seq(), Seq()) =>
          all_rels.map(rel =>
            (rel,
              if (includeStartNodes) {
                Some(nodeAt(rel.startId))
              }
              else {
                None
              },
              if (includeEndNodes) {
                Some(nodeAt(rel.endId))
              }
              else {
                None
              })
          ).iterator
      }
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

case class TestNode(id0: Long, labels: Set[String], props: (String, CypherValue)*) extends CypherNode {
  lazy val properties = props.toMap
  override val id: CypherId = TestCypherId(id0)
}

case class TestRelationship(id0: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends CypherRelationship {
  lazy val properties = props.toMap
  override val id: CypherId = TestCypherId(id0)
  override val startNodeId: CypherId = TestCypherId(startId)
  override val endNodeId: CypherId = TestCypherId(endId)
}
