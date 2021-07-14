import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.{CallableProcedure, ContextualNodeInputRef, CypherRunner, DefaultProcedureRegistry, DefaultProcedures, GraphModel, LynxId, LynxInteger, LynxList, LynxNode, LynxNull, LynxRelationship, LynxResult, LynxType, LynxValue, NodeFilter, NodeInput, NodeInputRef, PathTriple, ProcedureRegistry, RelationshipFilter, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, Range, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTInteger, CTString}

import scala.collection.mutable
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

    override def pathsWithLength(leftNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, rightNodeFilter: NodeFilter, direction: SemanticDirection, length: Option[Option[Range]]): Iterator[Seq[PathTriple]] = {
      /*
    length:
      [r:XXX] = None
      [r:XXX*] = Some(None) // degree 1 to MAX
      [r:XXX*..] =Some(Some(Range(None, None))) // degree 1 to MAX
      [r:XXX*..3] = Some(Some(Range(None, 3)))
      [r:XXX*1..] = Some(Some(Range(1, None)))
      [r:XXX*1..3] = Some(Some(Range(1, 3)))
============================================================================
      Seq[Seq[Seq[PathTriple]]]
        Seq(
          Seq(
            Seq(1->2)
            Seq(2->3)
          ),
          Seq(
            Seq(1->2, 2->3)
            Seq(4->5, 5->6)
          ),
          Seq(
            Seq(1->2, 2->3, 3->4),
            Seq(11->22, 22->33, 33->44)
          )
        )
     */
      def getPathHead(leftNodeFilter: NodeFilter, relsTriple: () => Iterator[PathTriple]): Iterator[Seq[PathTriple]] = {
        relsTriple().filter(f => leftNodeFilter.matches(f.startNode)).map(Seq(_))
      }

      def getPathMiddle(leftSide: Iterator[Seq[PathTriple]], relsTriple: () => Iterator[PathTriple]): Iterator[Seq[PathTriple]] = {
        if (leftSide.nonEmpty) {
          val res = leftSide.flatMap(leftTriple => {
            relsTriple().filter(p => p.startNode == leftTriple.last.endNode).filter(p => !leftTriple.contains(p)).map(f => leftTriple ++ Seq(f))
          })
          res.toList.distinct.toIterator // check circle
        }
        else Iterator.empty
      }

      def getBothPathMiddle(leftSide: Iterator[Seq[PathTriple]], relsTriple: () => Iterator[PathTriple]): Iterator[Seq[PathTriple]] = {
        if (leftSide.nonEmpty) {
          val res = leftSide.flatMap(leftTriple => {
            relsTriple().filter(
              p =>
                leftTriple.last.endNode == p.endNode || leftTriple.last.endNode == p.startNode ||
              leftTriple.last.startNode == p.startNode || leftTriple.last.startNode == p.endNode
            )
              .filter(p => !leftTriple.contains(p))
              .map(f => leftTriple ++ Seq(f))
          })
          res.toList.distinct.toIterator // check circle
        }
        else Iterator.empty
      }
      def getPathLast(leftList: Iterator[Seq[PathTriple]], rightNodeFilter: NodeFilter): Iterator[Seq[PathTriple]] = {
        if (leftList.nonEmpty) {
          leftList.filter(p => rightNodeFilter.matches(p.last.endNode))
        }
        else Iterator.empty
      }

      def getDegreeRelationship(lower: Int, upper: Int): Iterator[Seq[PathTriple]] = {
        val searchedPaths = ArrayBuffer[Iterator[Seq[PathTriple]]]()

        for (degree <- lower to upper) {
          direction match {
            case SemanticDirection.OUTGOING => {
              val relsTriple = () => relationships(relationshipFilter)
              degree match {
                case 0 => {
                  val res = nodes(leftNodeFilter).map(node => Seq(PathTriple(node, null, node)))
                  if (res.nonEmpty) searchedPaths += res
                }
                case 1 => {
                  val d1 = getPathHead(leftNodeFilter, relsTriple)
                  val res = d1.filter(p => rightNodeFilter.matches(p.head.endNode))
                  if (res.nonEmpty) searchedPaths += res
                }
                case n => {
                  val middleNum = n - 1
                  var left = getPathHead(leftNodeFilter, relsTriple)
                  for (i <- 1 to middleNum) {
                    left = getPathMiddle(left, relsTriple)
                  }
                  val res = getPathLast(left, rightNodeFilter)
                  if (res.nonEmpty) searchedPaths += res
                }
              }
            }
            case SemanticDirection.INCOMING => {
              // incoming: leftNodeFilter = right
              val relsTriple = () => relationships(relationshipFilter)
              degree match {
                case 0 => {
                  val res = nodes(rightNodeFilter).map(node => Seq(PathTriple(node, null, node)))
                  if (res.nonEmpty) searchedPaths += res
                }
                case 1 => {
                  val d1 = getPathHead(rightNodeFilter, relsTriple)
                  val res = d1.filter(p => leftNodeFilter.matches(p.head.endNode))
                  if (res.nonEmpty) searchedPaths += res
                }
                case n => {
                  val middleNum = n - 1
                  var left = getPathHead(rightNodeFilter, relsTriple)
                  for (i <- 1 to middleNum) {
                    left = getPathMiddle(left, relsTriple)
                  }
                  val res = getPathLast(left, leftNodeFilter)
                  if (res.nonEmpty) searchedPaths += res
                }
              }
            }
            case SemanticDirection.BOTH => {
              val relsTriple = () => relationships(relationshipFilter) // both out and in
              degree match {
               case 0 =>{
                 val res = nodes(leftNodeFilter).map(node => Seq(PathTriple(node, null, node)))
                 if (res.nonEmpty) searchedPaths += res
               }
               case 1 =>{
                 val out = getPathHead(leftNodeFilter, relsTriple).filter(p => rightNodeFilter.matches(p.head.endNode))
                 val in = getPathHead(rightNodeFilter, relsTriple).filter(p => leftNodeFilter.matches(p.head.endNode))
                 val res = (out ++ in).toList.distinct.toIterator
                 if (res.nonEmpty) searchedPaths += res
               }
               case n =>{
                 val middleNum = n - 1
                 val leftOut = getPathHead(leftNodeFilter, relsTriple).filter(p => rightNodeFilter.matches(p.head.endNode))
                 val leftIn = getPathHead(rightNodeFilter, relsTriple).filter(p => leftNodeFilter.matches(p.head.endNode))
                 var left = (leftOut ++ leftIn).toList.distinct.toIterator
                 for (i <- 1 to middleNum) {
                   left = getBothPathMiddle(left, relsTriple)
                 }
                 val resOut = getPathLast(left, rightNodeFilter)
                 val resIn = getPathLast(left, leftNodeFilter)
                 val res = (resOut ++ resIn).toList.distinct.toIterator
                 if (res.nonEmpty) searchedPaths += res
               }
             }
            }
          }
        }
        val res = searchedPaths.foldLeft(Iterator[Seq[PathTriple]]())((res, iterator) => res ++ iterator)
        res.toList.distinct.toIterator
      }

      length match {
        case Some(None) => getDegreeRelationship(1, 10) // upper tmp impl
        case Some(Some(range)) => {
          range match {
            case Range(None, None) => getDegreeRelationship(1, 10) // upper tmp impl
            case Range(lower, None) => getDegreeRelationship(lower.get.value.toInt, 10) // upper tmp impl
            case Range(None, upper) => getDegreeRelationship(1, upper.get.value.toInt)
            case Range(lower, upper) => getDegreeRelationship(lower.get.value.toInt, upper.get.value.toInt)
          }
        }
      }
    }

    override def estimateNodeLabel(labelName: String): Long = {
      allNodes.count(p => p.labels.contains(labelName))
    }

    override def estimateNodeProperty(propertyName: String): Long = {
      allNodes.count(p => p.property(propertyName).isDefined)
    }

    override def estimateRelationship(relType: String): Long = {
      allRelationships.count(p => p.relationType.get == relType)
    }

    override def copyNode(srcNode: LynxNode, maskNode: LynxNode): Seq[LynxValue] = {
      val _maskNode = maskNode.asInstanceOf[TestNode]
      val newSrcNode = TestNode(srcNode.id.value.asInstanceOf[Long], _maskNode.labels, _maskNode.properties.toSeq: _*)
      val index = allNodes.indexWhere(p => p.id == srcNode.id)
      allNodes(index) = newSrcNode
      Seq(newSrcNode, maskNode)
    }

    override def mergeNode(nodeFilter: NodeFilter, forceToCreate: Boolean): LynxNode = {
      if (forceToCreate) {
        val node = TestNode(allNodes.size + 1, nodeFilter.labels, nodeFilter.properties.toSeq: _*)
        allNodes.append(node)
        node
      }
      else {
        val checkMerged = nodes(nodeFilter)
        if (checkMerged.nonEmpty) {
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

      var currentNodeId = allNodes.size
      var currentRelId = allRelationships.size

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
        varname -> TestRelationship(currentRelId, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption, input.props: _*)
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


    override def setNodeProperty(nodeId: LynxId, data: Array[(String, LynxValue)], cleanExistProperties: Boolean): Option[LynxNode] = {
      val record = allNodes.find(n => n.id == nodeId)
      if (record.isDefined) {
        val node = record.get
        val newNode = {
          if (cleanExistProperties) {
            TestNode(node.id.value.asInstanceOf[Long], node.labels, data.toSeq: _*)
          }
          else {
            val prop = mutable.Map(node.properties.toSeq: _*)
            data.foreach(f => {
              prop(f._1) = LynxValue(f._2)
            })
            TestNode(node.id.value.asInstanceOf[Long], node.labels, prop.toSeq: _*)
          }
        }
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

    override def deleteRelations(ids: Iterator[LynxId]): Unit = {
      ids.foreach(rid => {
        deleteRelation(rid)
      })
    }

    override def deleteRelation(id: LynxId): Unit = {
      allRelationships --= allRelationships.filter(_.id == id)
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