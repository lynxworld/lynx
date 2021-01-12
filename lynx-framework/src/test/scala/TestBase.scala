import org.grapheco.lynx.util.Profiler
import org.grapheco.lynx._

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

  val runner = new CypherRunner(new GraphModel {
    override def createElements[T](
                                    nodes1: Array[(Option[String], NodeInput)],
                                    rels1: Array[(Option[String], RelationshipInput)],
                                    onCreated: (Map[Option[String], LynxNode], Map[Option[String], LynxRelationship]) => T): T = {
      val nodesMap: Map[NodeInput, (Option[String], TestNode)] = nodes1.map(x => {
        val (varname, input) = x
        val id = all_nodes.size + 1
        input -> (varname, TestNode(id, input.labels, input.props: _*))
      }).toMap

      def nodeId(ref: NodeInputRef): Long = {
        ref match {
          case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
          case ContextualNodeInputRef(node) => nodesMap(node)._2.longId
        }
      }

      val relsMap: Array[(Option[String], TestRelationship)] = rels1.map(x => {
        val (varname, input) = x
        varname -> TestRelationship(all_rels.size + 1, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
      }
      )

      all_nodes ++= nodesMap.map(_._2._2)
      all_rels ++= relsMap.map(_._2)

      onCreated(nodesMap.map(_._2), relsMap.toMap)
    }

    override def nodeAt(id: LynxId): Option[LynxNode] = {
      nodeAt(id.value.asInstanceOf[Long])
    }

    def nodeAt(id: Long): Option[LynxNode] = {
      all_nodes.find(_.longId == id)
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

case class TestNode(longId: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(longId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}

case class TestRelationship(id0: Long, startId: Long, endId: Long, relationType: Option[String], props: (String, LynxValue)*) extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(id0)
  override val startNodeId: LynxId = TestLynxId(startId)
  override val endNodeId: LynxId = TestLynxId(endId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}
