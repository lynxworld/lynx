package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.Profiler
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.util.symbols.CTString

import scala.collection.mutable.ArrayBuffer

class TestBase extends LazyLogging {
  Profiler.enableTiming = true

  //(bluejoe)-[:KNOWS]->(alex)-[:KNOWS]->(CNIC)
  //(bluejoe)-[]-(CNIC)
  val node1 = TestNode(1, Seq("person", "leader"), "name" -> LynxValue("bluejoe"), "age" -> LynxValue(40))
  val node2 = TestNode(2, Seq("person"), "name" -> LynxValue("alex"), "age" -> LynxValue(30))
  val node3 = TestNode(3, Seq(), "name" -> LynxValue("CNIC"), "age" -> LynxValue(50))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, Some("KNOWS")),
    TestRelationship(2, 2, 3, Some("KNOWS")),
    TestRelationship(3, 1, 3, None)
  )

  val NODE_SIZE = all_nodes.size
  val REL_SIZE = all_rels.size

  val model = new GraphModel {
    override def createElements[T](
      nodesInput: Seq[(String, NodeInput)],
      relsInput: Seq[(String, RelationshipInput)],
      onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
      val nodesMap: Seq[(String, TestNode)] = nodesInput.map(x => {
        val (varname, input) = x
        val id = all_nodes.size + 1
        varname -> TestNode(id, input.labels, input.props: _*)
      })

      def nodeId(ref: NodeInputRef): Long = {
        ref match {
          case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
          case ContextualNodeInputRef(varname) => nodesMap.find(_._1 == varname).get._2.longId
        }
      }

      val relsMap: Seq[(String, TestRelationship)] = relsInput.map(x => {
        val (varname, input) = x
        varname -> TestRelationship(all_rels.size + 1, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
      }
      )

      logger.debug(s"created nodes: ${nodesMap}, rels: ${relsMap}")
      all_nodes ++= nodesMap.map(_._2)
      all_rels ++= relsMap.map(_._2)

      onCreated(nodesMap, relsMap)
    }

    def nodeAt(id: Long): Option[LynxNode] = {
      all_nodes.find(_.longId == id)
    }

    override def nodes(): Iterator[LynxNode] = all_nodes.iterator

    override def relationships(): Iterator[PathTriple] =
      all_rels.map(rel =>
        PathTriple(nodeAt(rel.startId).get, rel, nodeAt(rel.endId).get)
      ).iterator

    override def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] = s"${prefix.mkString(".")}.${name}" match {
      case "test.authors" => Some(new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("name" -> CTString)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] =
          Seq(Seq(LynxValue("bluejoe")), Seq(LynxValue("lzx")), Seq(LynxValue("airzihao")))
      })

      case _ => None
    }
  }

  val runner = new CypherRunner(model)

  protected def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    println(s"query: $query")
    runner.compile(query)
    Profiler.timing {
      //call cache() only for test
      val rs = runner.run(query, param).cache()
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
