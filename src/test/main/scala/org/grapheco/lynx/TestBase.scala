package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.physical._
import org.grapheco.lynx.procedure.CallableProcedure
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.util.symbols.{CTInteger, CTString}

import scala.collection.mutable
import scala.language.implicitConversions

class TestBase extends LazyLogging {

  Profiler.enableTiming = true

  val all_nodes: mutable.Map[TestId, TestNode] = mutable.Map()

  val all_rels: mutable.Map[TestId, TestRelationship] = mutable.Map()

  var _nodeId: Long = 0

  var _relationshipId: Long = 0

  val _index: mutable.ArrayBuffer[Index] = mutable.ArrayBuffer()

  val model: GraphModel = new GraphModel {

    private def nodeId: TestId = {_nodeId += 1; TestId(_nodeId)}

    private def relationshipId: TestId = {_relationshipId += 1; TestId(_relationshipId)}

    def nodeAt(id: LynxId): Option[TestNode] = all_nodes.get(id)

    private def relationshipAt(id: LynxId): Option[TestRelationship] = all_rels.get(id)

    implicit def lynxId2myId(lynxId: LynxId): TestId = TestId(lynxId.value.asInstanceOf[Long])

    private val _writeTask: WriteTask = new WriteTask {

      val _nodesBuffer: mutable.Map[TestId, TestNode] = mutable.Map()

      val _nodesToDelete: mutable.ArrayBuffer[TestId] = mutable.ArrayBuffer()

      val _relationshipsBuffer: mutable.Map[TestId, TestRelationship] = mutable.Map()

      val _relationshipsToDelete: mutable.ArrayBuffer[TestId] = mutable.ArrayBuffer()

      private def updateNodes(ids: Iterator[LynxId], update: TestNode => TestNode): Iterator[Option[LynxNode]] = {
        ids.map{ id =>
          val updated = _nodesBuffer.get(id).orElse(nodeAt(id)).map(update)
          updated.foreach(newNode => _nodesBuffer.update(newNode.id, newNode))
          updated
        }
      }

      private def updateRelationships(ids: Iterator[LynxId], update: TestRelationship => TestRelationship): Iterator[Option[TestRelationship]] = {
        ids.map{ id =>
          val updated = _relationshipsBuffer.get(id).orElse(relationshipAt(id)).map(update)
          updated.foreach(newRel => _relationshipsBuffer.update(newRel.id, newRel))
          updated
        }
      }

      override def createElements[T](nodesInput: Seq[(String, NodeInput)],
                                     relationshipsInput: Seq[(String, RelationshipInput)],
                                     onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
        val nodesMap: Map[String, TestNode] = nodesInput
          .map{case (valueName,input) => valueName -> TestNode(nodeId, input.labels, input.props.toMap)}.toMap

        def localNodeRef(ref: NodeInputRef): TestId = ref match {
          case StoredNodeInputRef(id) => id
          case ContextualNodeInputRef(valueName) => nodesMap(valueName).id
        }

        val relationshipsMap: Map[String, TestRelationship] = relationshipsInput.map{
          case (valueName,input) =>
            valueName -> TestRelationship(relationshipId, localNodeRef(input.startNodeRef),
              localNodeRef(input.endNodeRef), input.types.headOption,input.props.toMap)
        }.toMap

        _nodesBuffer ++= nodesMap.map{ case (_, node) => (node.id, node)}
        _relationshipsBuffer ++= relationshipsMap.map{ case (_, relationship) => (relationship.id, relationship)}
        onCreated(nodesMap.toSeq, relationshipsMap.toSeq)
      }

      override def deleteRelations(ids: Iterator[LynxId]): Unit = ids.foreach{ id =>
        _relationshipsBuffer.remove(id)
        _relationshipsToDelete += id
      }

      override def deleteNodes(ids: Seq[LynxId]): Unit = ids.foreach{ id =>
        _nodesBuffer.remove(id)
        _nodesToDelete += id
      }

      override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)], cleanExistProperties: Boolean): Iterator[Option[LynxNode]] =
        updateNodes(nodeIds, old => TestNode(old.id, old.labels, if (cleanExistProperties) Map.empty else old.props ++ data.toMap.mapValues(LynxValue.apply)))

      override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] =
        updateNodes(nodeIds, old => TestNode(old.id, (old.labels ++ labels.toSeq).distinct, old.props))

      override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] =
        updateRelationships(relationshipIds, old => TestRelationship(old.id, old.startNodeId, old.endNodeId, old.relationType, data.toMap.mapValues(LynxValue.apply)))

      override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] =
        updateRelationships(relationshipIds, old => TestRelationship(old.id, old.startNodeId, old.endNodeId, Some(typeName), old.props))

      override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] =
        updateNodes(nodeIds, old => TestNode(old.id, old.labels, old.props.filterNot(p => data.contains(p._1))))

      override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] =
        updateNodes(nodeIds, old => TestNode(old.id, old.labels.filterNot(labels.contains), old.props))

      override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] =
        updateRelationships(relationshipIds, old => TestRelationship(old.id, old.startNodeId, old.endNodeId, old.relationType, old.props.filterNot(p => data.contains(p._1))))

      override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] =
        updateRelationships(relationshipIds, old => TestRelationship(old.id, old.startNodeId, old.endNodeId, None, old.props))

      override def commit: Boolean = {
        all_nodes ++= _nodesBuffer
        all_nodes --= _nodesToDelete
        all_rels ++= _relationshipsBuffer
        all_rels --= _relationshipsToDelete
        _nodesBuffer.clear()
        _nodesToDelete.clear()
        _relationshipsBuffer.clear()
        _relationshipsToDelete.clear()
        true
      }
    }

    override def write: WriteTask = _writeTask

    override def indexManager: IndexManager = new IndexManager {
      override def createIndex(index: Index): Unit = _index += index

      override def dropIndex(index: Index): Unit = _index -= index

      override def indexes: Array[Index] = _index.toArray
    }

    override def statistics: Statistics = new Statistics {
      override def numNode: Long = nodes().length

      override def numNodeByLabel(labelName: LynxNodeLabel): Long =
        all_nodes.count(_._2.labels.contains(labelName))

      override def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long =
        nodes(NodeFilter(Seq(labelName), Map(propertyName->value))).length

      override def numRelationship: Long = relationships().length

      override def numRelationshipByType(typeName: LynxRelationshipType): Long =
        all_rels.count(_._2.relationType.forall(typeName.equals))
    }

    override def nodes(): Iterator[LynxNode] = all_nodes.valuesIterator

    override def relationships(): Iterator[PathTriple] =
      all_rels.iterator.map{ case(_, rel) => PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get)}
  }

  val runner: CypherRunner = new CypherRunner(model) {

    procedures.register("test.authors", 0, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq()
      override val outputs: Seq[(String, LynxType)] = Seq("name" -> CTString)

      override def call(args: Seq[LynxValue]): LynxValue =
        LynxList(List(LynxValue("bluejoe"), LynxValue("lzx"), LynxValue("airzihao")))
    })

    procedures.register("toInterger", 1, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq("text" -> CTString)
      override val outputs: Seq[(String, LynxType)] = Seq("number" -> CTInteger)

      override def call(args: Seq[LynxValue]): LynxValue =
        LynxInteger(args.head.value.toString.toInt)
    })
  }

  protected def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    //runner.compile(query)
    Profiler.timing("Run On Demo Graph",
      {
        val rs = runner.run(query, param).cache()
        rs.show()
        rs
      }
    )
  }

  case class TestId(value: Long) extends LynxId {
    override def toLynxInteger: LynxInteger = LynxInteger(value)
  }

  case class TestNode(id: TestId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]) extends LynxNode{
    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

    override def keys: Seq[LynxPropertyKey] = props.keys.toSeq

  }

  case class TestRelationship(id: TestId,
                              startNodeId: TestId,
                              endNodeId: TestId,
                              relationType: Option[LynxRelationshipType],
                              props: Map[LynxPropertyKey, LynxValue]) extends LynxRelationship {
    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)
    override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
  }
}