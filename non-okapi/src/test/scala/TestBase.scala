import org.grapheco.lynx.{LynxId, LynxNode, LynxRelationship, LynxResult, LynxRunner, LynxValue, GraphModel, Node2Create, Relationship2Create}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{LabelName, SemanticDirection}

import scala.collection.mutable.ArrayBuffer

class TestBase {
  Profiler.enableTiming = true

  val node1 = TestNode(1, Seq("person", "leader"), "name" -> LynxValue("bluejoe"), "age" -> LynxValue(40))
  val node2 = TestNode(2, Seq("person"), "name" -> LynxValue("alex"), "age" -> LynxValue(30))
  val node3 = TestNode(3, Seq(), "name" -> LynxValue("simba"), "age" -> LynxValue(10))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, Some("knows")),
    TestRelationship(2, 2, 3, Some("knows")),
    TestRelationship(3, 1, 3, None)
  )

  val runner = new LynxRunner(new GraphModel {
    override def createElements(nodes: Array[Node2Create], rels: Array[Relationship2Create]): Unit = ???

    override def nodeAt(id: LynxId): Option[LynxNode] = {
      nodeAt(id.value.asInstanceOf[Long])
    }

    def nodeAt(id: Long): Option[LynxNode] = {
      all_nodes.find(_.id0 == id)
    }

    override def nodes(): Iterator[LynxNode] = all_nodes.iterator

    override def rels(
                       includeStartNodes: Boolean,
                       includeEndNodes: Boolean
                     ): Iterator[(LynxRelationship, Option[LynxNode], Option[LynxNode])] = {
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

  def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    println(s"query: $query")
    runner.compile(query)
    Profiler.timing {
      val rs = runner.run(query, param)
      rs.show()
      rs
    }
  }

}

case class TestLynxId(value: Long) extends LynxId {
}

case class TestNode(id0: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(id0)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}

case class TestRelationship(id0: Long, startId: Long, endId: Long, relationType: Option[String], props: (String, LynxValue)*) extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(id0)
  override val startNodeId: LynxId = TestLynxId(startId)
  override val endNodeId: LynxId = TestLynxId(endId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}
