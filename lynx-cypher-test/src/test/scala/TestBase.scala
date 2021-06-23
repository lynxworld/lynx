import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.{CallableProcedure, ContextualNodeInputRef, CypherRunner, DefaultProcedureRegistry, DefaultProcedures, GraphModel, LynxId, LynxInteger, LynxList, LynxNode, LynxRelationship, LynxResult, LynxType, LynxValue, NodeFilter, NodeInput, NodeInputRef, PathTriple, ProcedureRegistry, RelationshipFilter, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTInteger, CTString}

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 10:58 2021/5/26
 * @Modified By:
 */

class TestBase(allNodes: ArrayBuffer[TestNode], allRelationships: ArrayBuffer[TestRelationship]) extends LazyLogging {
  Profiler.enableTiming = true

  val allIndex = ArrayBuffer[(LabelName, List[PropertyKeyName])]()

  val NODE_SIZE = allNodes.size
  val REL_SIZE = allRelationships.size

  val model = new GraphModel {

    override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean): LynxNode = {
      // 1.
      if (forceToCreate){
        val node = TestNode(allNodes.size + 1, nodeFilter.labels, nodeFilter.properties.toSeq: _*)
        allNodes.append(node)
        node
      }
      else {
        val checkMerged = nodes(nodeFilter)
        if (checkMerged.nonEmpty){
          checkMerged.next()
        }
        else {
          val node = TestNode(allNodes.size + 1, nodeFilter.labels, nodeFilter.properties.toSeq: _*)
          allNodes.append(node)
          node
        }
      }
    }

    override def mergeRelationship(relationshipFilter: RelationshipFilter, leftNode: LynxNode, rightNode: LynxNode, direction: SemanticDirection, forceToCreate: Boolean): PathTriple = {
      val relationship = direction match {
        case SemanticDirection.INCOMING => {
          val r1 = TestRelationship(allRelationships.size + 1, rightNode.id.value.asInstanceOf[Long], leftNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          allRelationships.append(r1)
          r1
        }
        case SemanticDirection.OUTGOING => {
          val r1 = TestRelationship(allRelationships.size + 1, leftNode.id.value.asInstanceOf[Long], rightNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          allRelationships.append(r1)
          r1
        }
        case SemanticDirection.BOTH => {
          val r1 = TestRelationship(allRelationships.size + 1, leftNode.id.value.asInstanceOf[Long], rightNode.id.value.asInstanceOf[Long], relationshipFilter.types.headOption, relationshipFilter.properties.toSeq: _*)
          allRelationships.append(r1)
          r1
        }
      }
      PathTriple(leftNode, relationship, rightNode)
    }

  override def createElements[T](
                                  nodesInput: Seq[(String, NodeInput)],
                                  relsInput: Seq[(String, RelationshipInput)],
                                  onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
    val nodesMap: Seq[(String, TestNode)] = nodesInput.map(x => {
      val (varname, input) = x
      val id = allNodes.size + 1
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
      varname -> TestRelationship(allRelationships.size + 1, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
    }
    )

    logger.debug(s"created nodes: ${nodesMap}, rels: ${relsMap}")
    allNodes ++= nodesMap.map(_._2)
    allRelationships ++= relsMap.map(_._2)

    onCreated(nodesMap, relsMap)
  }

  def nodeAt(id: Long): Option[LynxNode] = {
    allNodes.find(_.longId == id)
  }

  override def nodes(): Iterator[LynxNode] = allNodes.iterator

  override def relationships(): Iterator[PathTriple] =
    allRelationships.map(rel =>
      PathTriple(nodeAt(rel.startId).get, rel, nodeAt(rel.endId).get)
    ).iterator

  override def createIndex(labelName: LabelName, properties: List[PropertyKeyName]): Unit = {
    allIndex += Tuple2(labelName, properties)
  }

  override def getIndexes(): Array[(LabelName, List[PropertyKeyName])] = {
    allIndex.toArray
  }

  override def filterNodesWithRelations(nodesIDs: Seq[LynxId]): Seq[LynxId] = {
    nodesIDs.filter(id => allRelationships.filter(rel => rel.startNodeId == id || rel.endNodeId == id).nonEmpty)
  }

  override def deleteRelationsOfNodes(nodesIDs: Seq[LynxId]): Unit = {
    nodesIDs.foreach(id => allRelationships --= allRelationships.filter(rel => rel.startNodeId == id || rel.endNodeId == id))
  }

  override def deleteFreeNodes(nodesIDs: Seq[LynxId]): Unit = {
    nodesIDs.foreach(id => allNodes --= allNodes.filter(_.id == id))
  }


  override def setNodeProperty(nodeId: LynxId, data: Array[(String, AnyRef)]): Option[LynxNode] = {
    val record = allNodes.find(n => n.id == nodeId)
    if (record.isDefined) {
      val node = record.get
      val property = scala.collection.mutable.Map(node.properties.toSeq: _*)
      data.foreach(f => property(f._1) = LynxValue(f._2))
      val newNode = TestNode(node.id.value.asInstanceOf[Long], node.labels, property.toSeq: _*)
      val index = allNodes.indexWhere(p => p == node)
      allNodes(index) = newNode
      Option(newNode)
    }
    else None
  }

  override def addNodeLabels(nodeId: LynxId, labels: Array[String]): Option[LynxNode] = {
    val record = allNodes.find(n => n.id == nodeId)
    if (record.isDefined) {
      val node = record.get
      val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels ++ labels).distinct, node.properties.toSeq: _*)
      val index = allNodes.indexWhere(p => p == node)
      allNodes(index) = newNode
      Option(newNode)
    }
    else None
  }

  override def setRelationshipProperty(triple: Seq[LynxValue], data: Array[(String, AnyRef)]): Option[Seq[LynxValue]] = {
    val rel = triple(1).asInstanceOf[LynxRelationship]
    val record = allRelationships.find(r => r.id == rel.id)
    if (record.isDefined) {
      val relation = record.get
      val property = scala.collection.mutable.Map(relation.properties.toSeq: _*)
      data.foreach(f => property(f._1) = LynxValue(f._2))
      val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq: _*)
      val index = allRelationships.indexWhere(p => p == relation)
      allRelationships(index) = newRelationship
      Option(Seq(triple.head, newRelationship, triple(2)))
    }
    else None
  }

  override def setRelationshipTypes(triple: Seq[LynxValue], labels: Array[String]): Option[Seq[LynxValue]] = {
    val rel = triple(1).asInstanceOf[LynxRelationship]
    val record = allRelationships.find(r => r.id == rel.id)
    if (record.isDefined) {
      val relation = record.get
      val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, Option(labels.head), relation.properties.toSeq: _*)
      val index = allRelationships.indexWhere(p => p == relation)
      allRelationships(index) = newRelationship
      Option(Seq(triple.head, newRelationship, triple(2)))
    }
    else None
  }

  override def removeNodeProperty(nodeId: LynxId, data: Array[String]): Option[LynxNode] = {
    val record = allNodes.find(n => n.id == nodeId)
    if (record.isDefined) {
      val node = record.get
      val property = scala.collection.mutable.Map(node.properties.toSeq: _*)
      data.foreach(f => {
        if (property.contains(f)) property -= f
      })
      val newNode = TestNode(node.id.value.asInstanceOf[Long], node.labels, property.toSeq: _*)

      val index = allNodes.indexWhere(p => p == node)
      allNodes(index) = newNode
      Option(newNode)
    }
    else None
  }

  override def removeNodeLabels(nodeId: LynxId, labels: Array[String]): Option[LynxNode] = {
    val record = allNodes.find(n => n.id == nodeId)
    if (record.isDefined) {
      val node = record.get
      val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels.toBuffer -- labels), node.properties.toSeq: _*)
      val index = allNodes.indexWhere(p => p == node)
      allNodes(index) = newNode
      Option(newNode)
    }
    else None
  }

  override def removeRelationshipProperty(triple: Seq[LynxValue], data: Array[String]): Option[Seq[LynxValue]] = {
    val rel = triple(1).asInstanceOf[LynxRelationship]
    val record = allRelationships.find(r => r.id == rel.id)
    if (record.isDefined) {
      val relation = record.get
      val property = scala.collection.mutable.Map(relation.properties.toSeq: _*)
      data.foreach(f => {
        if (property.contains(f)) property -= f
      })
      val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq: _*)
      val index = allRelationships.indexWhere(p => p == relation)
      allRelationships(index) = newRelationship
      Option(Seq(triple.head, newRelationship, triple(2)))
    }
    else None
  }

  override def removeRelationshipType(triple: Seq[LynxValue], labels: Array[String]): Option[Seq[LynxValue]] = {
    val rel = triple(1).asInstanceOf[LynxRelationship]
    val record = allRelationships.find(r => r.id == rel.id)
    if (record.isDefined) {
      val relation = record.get
      val newType: Option[String] = {
        if (relation.relationType.get == labels.head) None
        else Option(relation.relationType.get)
      }
      val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, newType, relation.properties.toSeq: _*)
      val index = allRelationships.indexWhere(p => p == relation)
      allRelationships(index) = newRelationship
      Option(Seq(triple.head, newRelationship, triple(2)))
    }
    else None
  }
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

    override def call(args: Seq[LynxValue]): LynxValue =
      LynxInteger(args.head.value.toString.toInt)
  })
}

def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
  runner.compile(query)
  Profiler.timing {
    //call cache() only for test
    val rs = runner.run(query, param).cache()
    rs.show()
    rs
  }
}
}