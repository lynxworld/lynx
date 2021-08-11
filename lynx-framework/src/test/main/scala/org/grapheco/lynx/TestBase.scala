package org.grapheco.lynx

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZoneId, ZoneOffset, ZonedDateTime}

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTAny, CTDate, CTDateTime, CTInteger, CTLocalDateTime, CTLocalTime, CTString, CTTime}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TestBase extends LazyLogging {
  Profiler.enableTiming = true

  //(bluejoe)-[:KNOWS]->(alex)-[:KNOWS]->(CNIC)
  //(bluejoe)-[]-(CNIC)
  val node1 = TestNode(1, Seq("person", "leader"), "gender" -> LynxValue("male"), "name" -> LynxValue("bluejoe"), "age" -> LynxValue(40))
  val node2 = TestNode(2, Seq("person"), "name" -> LynxValue("Alice"), "gender" -> LynxValue("female"), "age" -> LynxValue(30))
  val node3 = TestNode(3, Seq(), "name" -> LynxValue("Bob"), "gender" -> LynxValue("male"), "age" -> LynxValue(10))
  val node4 = TestNode(4, Seq(), "name" -> LynxValue("Bob2"), "gender" -> LynxValue("male"), "age" -> LynxValue(10))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3, node4)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, Some("KNOWS"), "years" -> LynxValue(5)),
    TestRelationship(2, 2, 3, Some("KNOWS"), "years" -> LynxValue(4)),
    TestRelationship(4, 3, 4, Some("KNOWS")),
    TestRelationship(3, 1, 3, None)
  )
  val allIndex = ArrayBuffer[(LabelName, List[PropertyKeyName])]()

  val NODE_SIZE = all_nodes.size
  val REL_SIZE = all_rels.size

  val model = new GraphModel {

    override def estimateNodeLabel(labelName: String): Long = {
      all_nodes.count(p => p.labels.contains(labelName))
    }

    override def estimateNodeProperty(propertyName: String, value: AnyRef): Long = {
      all_nodes.count(p => {
        val a = p.property(propertyName)
        if (a.isDefined){
          if (a.get.value == value) true
          else false
        }
        else false
      })
    }

    override def estimateRelationship(relType: String): Long = {
      all_rels.count(p => p.relationType.get == relType)
    }

    override def copyNode(srcNode: LynxNode, maskNode: LynxNode): Seq[LynxValue] = {
      val _maskNode = maskNode.asInstanceOf[TestNode]
      val newSrcNode = TestNode(srcNode.id.value.asInstanceOf[Long], _maskNode.labels, _maskNode.properties.toSeq:_*)
      val index = all_nodes.indexWhere(p => p.id == srcNode.id)
      all_nodes(index) = newSrcNode
      Seq(newSrcNode, maskNode)
    }

    override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean): LynxNode = {
      if (forceToCreate){
        val node = TestNode(all_nodes.size + 1, nodeFilter.labels, nodeFilter.properties.toSeq: _*)
        all_nodes.append(node)
        node
      }
      else {
        val checkMerged = nodes(nodeFilter)
        if (checkMerged.nonEmpty){
          checkMerged.next()
        }
        else {
          val node = TestNode(all_nodes.size + 1, nodeFilter.labels, nodeFilter.properties.toSeq: _*)
          all_nodes.append(node)
          node
        }
      }
    }

    override def mergeRelationship(relationshipFilter: RelationshipFilter, leftNode: LynxNode, rightNode: LynxNode, direction: SemanticDirection, forceToCreate: Boolean): PathTriple = {
      val relationship = direction match {
        case SemanticDirection.INCOMING => {
          val r1 = TestRelationship(all_rels.size + 1, rightNode.id.value.asInstanceOf[Long], leftNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          all_rels.append(r1)
          r1
        }
        case SemanticDirection.OUTGOING => {
          val r1 = TestRelationship(all_rels.size + 1, leftNode.id.value.asInstanceOf[Long], rightNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          all_rels.append(r1)
          r1
        }
        case SemanticDirection.BOTH => {
          val r1 = TestRelationship(all_rels.size + 1, leftNode.id.value.asInstanceOf[Long], rightNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          all_rels.append(r1)
          r1
        }
      }
      PathTriple(leftNode, relationship, rightNode)
    }

    override def createElements[T](
      nodesInput: Seq[(String, NodeInput)],
      relsInput: Seq[(String, RelationshipInput)],
      onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {

      var currentNodeId = all_nodes.size
      var currentRelId = all_rels.size

      val nodesMap: Seq[(String, TestNode)] = nodesInput.map(x => {
        val (varname, input) = x
        currentNodeId += 1
        varname -> TestNode(currentNodeId, input.labels, input.props: _*)
      })

      def nodeId(ref: NodeInputRef): Long = {
        ref match {
          case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
          case ContextualNodeInputRef(varname) => nodesMap.find(_._1 == varname).get._2.longId
        }
      }

      val relsMap: Seq[(String, TestRelationship)] = relsInput.map(x => {
        val (varname, input) = x
        currentRelId += 1
        varname -> TestRelationship(currentRelId, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption, input.props:_*)
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

    override def createIndex(labelName: LabelName, properties: List[PropertyKeyName]): Unit = {
      allIndex += Tuple2(labelName, properties)
    }

    override def getIndexes(): Array[(LabelName, List[PropertyKeyName])] = {
      allIndex.toArray
    }

    override def filterNodesWithRelations(nodesIDs: Seq[LynxId]): Seq[LynxId] = {
      nodesIDs.filter(id => all_rels.filter(rel => rel.startNodeId==id || rel.endNodeId==id).nonEmpty)
    }

    override def deleteRelationsOfNodes(nodesIDs: Seq[LynxId]): Unit = {
      nodesIDs.foreach(id => all_rels --= all_rels.filter(rel => rel.startNodeId==id || rel.endNodeId==id))
    }

    override def deleteFreeNodes(nodesIDs: Seq[LynxId]): Unit = {
      nodesIDs.foreach(id => all_nodes --= all_nodes.filter(_.id==id))
    }


    override def setNodeProperty(nodeId: LynxId, data: Array[(String, Any)], cleanExistProperties: Boolean): Option[LynxNode] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined) {
        val node = record.get
        val newNode = {
          if (cleanExistProperties){
            TestNode(node.id.value.asInstanceOf[Long], node.labels, data.map(f => (f._1, LynxValue(f._2))):_*)
          }
          else {
            val prop = mutable.Map(node.properties.toSeq:_*)
            data.foreach(f => {
              prop(f._1) = LynxValue(f._2)
            })
            TestNode(node.id.value.asInstanceOf[Long], node.labels, prop.toSeq: _*)
          }
        }
        val index = all_nodes.indexWhere(p => p == node)
        all_nodes(index) = newNode
        Option(newNode)
      }
      else None
    }

    override def addNodeLabels(nodeId: LynxId, labels: Array[String]): Option[LynxNode] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels ++ labels).distinct, node.properties.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        Option(newNode)
      }
      else None
    }

    override def setRelationshipProperty(triple: Seq[LynxValue], data: Array[(String ,Any)]): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val property = scala.collection.mutable.Map(relation.properties.toSeq:_*)
        data.foreach(f => property(f._1) = LynxValue(f._2))
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        Option(Seq(triple.head, newRelationship, triple(2)))
      }
      else None
    }

    override def setRelationshipTypes(triple: Seq[LynxValue], labels: Array[String]): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, Option(labels.head), relation.properties.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        Option(Seq(triple.head, newRelationship, triple(2)))
      }
      else None
    }

    override def removeNodeProperty(nodeId: LynxId, data: Array[String]): Option[LynxNode] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val property = scala.collection.mutable.Map(node.properties.toSeq:_*)
        data.foreach(f => {if (property.contains(f)) property -= f} )
        val newNode = TestNode(node.id.value.asInstanceOf[Long], node.labels, property.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        Option(newNode)
      }
      else None
    }

    override def removeNodeLabels(nodeId: LynxId, labels: Array[String]): Option[LynxNode] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels.toBuffer -- labels), node.properties.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        Option(newNode)
      }
      else None
    }

    override def removeRelationshipProperty(triple: Seq[LynxValue], data: Array[String]): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val property = scala.collection.mutable.Map(relation.properties.toSeq:_*)
        data.foreach(f => {if (property.contains(f)) property -= f} )
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        Option(Seq(triple.head, newRelationship, triple(2)))
      }
      else None
    }

    override def removeRelationshipType(triple: Seq[LynxValue], labels: Array[String]): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val newType: Option[String] = {
          if (relation.relationType.get == labels.head) None
          else Option(relation.relationType.get)
        }
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, newType, relation.properties.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        Option(Seq(triple.head, newRelationship, triple(2)))
      }
      else None
    }

    override def deleteRelations(ids: Iterator[LynxId]): Unit = {
      ids.foreach(rid =>{
        deleteRelation(rid)
      })
    }
    override def deleteRelation(id: LynxId): Unit = {
      all_rels --= all_rels.filter(_.id == id)
    }

    override def pathsWithLength(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection, length: Option[Option[expressions.Range]]): Iterator[Seq[PathTriple]] = ???
  }

  val runner = new CypherRunner(model) {
    val myfun = new DefaultProcedureRegistry(types, classOf[DefaultProcedures])
    override lazy val procedures: ProcedureRegistry = myfun

    myfun.register("test.authors", 0, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq()
      override val outputs: Seq[(String, LynxType)] = Seq("name" -> CTString)

      override def call(args: Seq[LynxValue]): LynxValue =
        LynxList(List(LynxValue("bluejoe"), LynxValue("lzx"), LynxValue("airzihao")))
    })

    myfun.register("toInterger", 1, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq("text" -> CTString)
      override val outputs: Seq[(String, LynxType)] = Seq("number" -> CTInteger)

      override def call(args: Seq[LynxValue]): LynxValue=
        LynxInteger(args.head.value.toString.toInt)
    })
  }

  protected def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    runner.compile(query)
    Profiler.timing {
      //call cache() only for test
      val rs = runner.run(query, param).cache()
      rs.show()
      rs
    }
  }
  protected def runOnDemoGraph2(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    runner.compile(query)
    Profiler.timing {
      //call cache() only for test
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