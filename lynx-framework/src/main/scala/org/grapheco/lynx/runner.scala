
package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, Range, SemanticDirection}
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import scala.annotation.tailrec
import scala.collection.mutable

case class CypherRunnerContext(typeSystem: TypeSystem,
                               procedureRegistry: ProcedureRegistry,
                               dataFrameOperator: DataFrameOperator,
                               expressionEvaluator: ExpressionEvaluator,
                               graphModel: GraphModel)

class CypherRunner(graphModel: GraphModel) extends LazyLogging {
  protected lazy val types: TypeSystem = new DefaultTypeSystem()
  protected lazy val procedures: ProcedureRegistry = new DefaultProcedureRegistry(types, classOf[DefaultProcedures])
  protected lazy val expressionEvaluator: ExpressionEvaluator = new DefaultExpressionEvaluator(graphModel, types, procedures)
  protected lazy val dataFrameOperator: DataFrameOperator = new DefaultDataFrameOperator(expressionEvaluator)
  private implicit lazy val runnerContext = CypherRunnerContext(types, procedures, dataFrameOperator, expressionEvaluator, graphModel)
  protected lazy val logicalPlanner: LogicalPlanner = new DefaultLogicalPlanner(runnerContext)
  protected lazy val physicalPlanner: PhysicalPlanner = new DefaultPhysicalPlanner(runnerContext)
  protected lazy val physicalPlanOptimizer: PhysicalPlanOptimizer = new DefaultPhysicalPlanOptimizer(runnerContext)
  protected lazy val queryParser: QueryParser = new CachedQueryParser(new DefaultQueryParser(runnerContext))

  def compile(query: String): (Statement, Map[String, Any], SemanticState) = queryParser.parse(query)

  def run(query: String, param: Map[String, Any], tx: Option[LynxTransaction]): LynxResult = {
    val (statement, param2, state) = queryParser.parse(query)
    logger.debug(s"AST tree: ${statement}")

    val logicalPlannerContext = LogicalPlannerContext(param ++ param2, runnerContext)
    val logicalPlan = logicalPlanner.plan(statement, logicalPlannerContext)
    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    val physicalPlannerContext = PhysicalPlannerContext(param ++ param2, runnerContext)
    val physicalPlan = physicalPlanner.plan(logicalPlan)(physicalPlannerContext)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val optimizedPhysicalPlan = physicalPlanOptimizer.optimize(physicalPlan, physicalPlannerContext, tx)
    logger.debug(s"optimized physical plan: \r\n${optimizedPhysicalPlan.pretty}")

    val ctx = ExecutionContext(physicalPlannerContext, statement, param ++ param2, tx)
    val df = optimizedPhysicalPlan.execute(ctx)

    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames,
          df.records.take(limit).toSeq.map(_.map(_.value)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[Map[String, Any]] = df.records.map(columnNames.zip(_).toMap)

      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)

      override def getLogicalPlan(): LPTNode = logicalPlan

      override def getPhysicalPlan(): PPTNode = physicalPlan

      override def getOptimizerPlan(): PPTNode = optimizedPhysicalPlan

      override def cache(): LynxResult = {
        val source = this
        val cached = df.records.toSeq

        new LynxResult {
          override def show(limit: Int): Unit = FormatUtils.printTable(columnNames,
            cached.take(limit).toSeq.map(_.map(_.value)))

          override def cache(): LynxResult = this

          override def columns(): Seq[String] = columnNames

          override def records(): Iterator[Map[String, Any]] = cached.map(columnNames.zip(_).toMap).iterator

        }
      }
    }
  }

}

//TODO: LogicalPlannerContext vs. PhysicalPlannerContext?
object LogicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): LogicalPlannerContext =
    new LogicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.cypherType).toSeq,
      runnerContext)
}

case class LogicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext)

object PhysicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): PhysicalPlannerContext =
    new PhysicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.cypherType).toSeq,
      runnerContext)
}

case class PhysicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext, var pptContext: mutable.Map[String, Any]=mutable.Map.empty) {
}

//TODO: context.context??
case class ExecutionContext(physicalPlannerContext: PhysicalPlannerContext, statement: Statement, queryParameters: Map[String, Any], tx: Option[LynxTransaction]) {
  val expressionContext = ExpressionContext(this, queryParameters.map(x => x._1 -> physicalPlannerContext.runnerContext.typeSystem.wrap(x._2)))
}

trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[Map[String, Any]]
}

trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LPTNode

  def getPhysicalPlan(): PPTNode

  def getOptimizerPlan(): PPTNode
}

/**
 * labels note: the node with both LABEL1 and LABEL2 labels.
 * @param labels label names
 * @param properties filter property names
 */
case class NodeFilter(labels: Seq[String], properties: Map[String, LynxValue]) {
  def matches(node: LynxNode): Boolean = labels.forall(node.labels.map(_.name).contains) &&
    properties.forall { case (propertyName, value) => node.property(propertyName).exists(value.equals) }
}

/**
 * types note: the relationship of type TYPE1 or of type TYPE2.
 * @param types type names
 * @param properties filter property names
 */
case class RelationshipFilter(types: Seq[String], properties: Map[String, LynxValue]) {
  def matches(relationship: LynxRelationship): Boolean = ((types, relationship.relationType) match {
    case (Seq(), _) => true
    case (_, None) => false
    case (_, Some(typeName)) => types.map(LynxRelationshipType.fromName).contains(typeName) // TODO: how to parse String to RelationshipType
  }) && properties.forall { case (propertyName, value) => relationship.property(propertyName).exists(value.equals) }
}

case class PathTriple(startNode: LynxNode, storedRelation: LynxRelationship, endNode: LynxNode, reverse: Boolean = false) {
  def revert: PathTriple = PathTriple(endNode, storedRelation, startNode, !reverse)
}

trait Statistics {

  def nodeNumber: Long

  def nodeNumberOfLabel(labelName: LynxNodeLabel): Long

  def nodeNumberOfProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long

  def relationshipNumber: Long

  def relationshipNumberOfType(typeName: LynxRelationshipType): Long
}

case class Index(labelName: LynxNodeLabel, properties: Set[LynxPropertyKey])

trait IndexManager {

  def createIndex(index: Index): Unit

  def dropIndex(index: Index): Unit

  def indexes: Array[Index]
}

trait WriteTask {

  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T

  def deleteRelations(ids: Iterator[LynxId]): Unit

  def deleteNodes(ids: Seq[LynxId]): Unit

  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey ,Any)], cleanExistProperties: Boolean = false): LynxNode

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): LynxNode

  def setRelationshipsProperties(relationships: Iterator[LynxRelationship],  data: Array[(LynxPropertyKey ,Any)]): LynxValue

  def setRelationshipsType(relationships: Iterator[LynxRelationship], typeName: LynxRelationshipType): LynxValue

  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): LynxNode

  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): LynxNode

  def removeRelationshipsProperties(relationships: Iterator[LynxRelationship],  data: Array[LynxPropertyKey]): LynxValue

  def removeRelationshipsType(relationships: Iterator[LynxRelationship], typeName: LynxRelationshipType): LynxValue

  def commit: Boolean

}


trait GraphModel{

  /**
   *
   * @return
   */
  def statistics: Statistics = new Statistics {
    override def nodeNumber: Long = 0

    override def nodeNumberOfLabel(labelName: LynxNodeLabel): Long = 0

    override def nodeNumberOfProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long = 0

    override def relationshipNumber: Long = 0

    override def relationshipNumberOfType(typeName: LynxRelationshipType): Long = 0
  }

  /**
   *
   * @return
   */
  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index creation")

    override def dropIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index dropping")

    override def indexes: Array[Index] = Array.empty
  }

  /**
   *
   * @return
   */
  def writeTask: WriteTask

  /*
    Parse the name string to value of labels, types and propertyKeys
   */
  def toLabel(name: String): LynxNodeLabel = LynxNodeLabel.fromName(name)

  def toType(name: String): LynxRelationshipType = LynxRelationshipType.fromName(name)

  def toPropertyKey(name: String): LynxPropertyKey = LynxPropertyKey.fromName(name)

  /**
   *
   * @return
   */
  def nodes(): Iterator[LynxNode]

  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches)

  /**
   *
   * @return
   */
  def relationships(): Iterator[PathTriple]

  def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = relationships().filter(f => relationshipFilter.matches(f.storedRelation))

  def nodeCount: Long = nodes().length

  def relationshipsCount: Long = relationships().length

  /*
    Create and delete of nodes and relationships.
    Write operations about label, type and property.
   */
  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T =
    this.writeTask.createElements(nodesInput, relationshipsInput, onCreated)

  def deleteRelations(ids: Iterator[LynxId]): Unit = this.writeTask.deleteRelations(ids)

  def deleteNodes(ids: Seq[LynxId]): Unit = this.writeTask.deleteNodes(ids)

  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(String ,Any)], cleanExistProperties: Boolean = false): LynxNode =
    this.writeTask.setNodesProperties(nodeIds, data.map(kv => (toPropertyKey(kv._1), kv._2)), cleanExistProperties)

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): LynxNode =
    this.writeTask.setNodesLabels(nodeIds, labels.map(toLabel))

  def setRelationshipsProperties(relationships: Iterator[LynxRelationship],  data: Array[(String ,Any)]): LynxValue =
    this.writeTask.setRelationshipsProperties(relationships, data.map(kv => (toPropertyKey(kv._1), kv._2)))

  def setRelationshipsType(relationships: Iterator[LynxRelationship], theType: String): LynxValue =
    this.writeTask.setRelationshipsType(relationships, toType(theType))

  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[String]): LynxNode =
    this.writeTask.removeNodesProperties(nodeIds, data.map(toPropertyKey))

  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): LynxNode =
    this.writeTask.removeNodesLabels(nodeIds, labels.map(toLabel))

  def removeRelationshipsProperties(relationships: Iterator[LynxRelationship],  data: Array[String]): LynxValue =
    this.writeTask.removeRelationshipsProperties(relationships, data.map(toPropertyKey))

  def removeRelationshipType(relationships: Iterator[LynxRelationship], theType: String): LynxValue =
    this.writeTask.removeRelationshipsType(relationships, toType(theType))

  def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = relationships().map(_.storedRelation)
      .filter(rel => ids.contains(rel.startNodeId) || ids.contains(rel.endNodeId))
    if (affectedRelationships.nonEmpty) {
      if (forced)
        deleteRelations(affectedRelationships.map(_.id))
      else
        throw ConstrainViolatedException(s"deleting referred nodes")
    }
    deleteNodes(ids.toSeq)
  }

  def commit: Boolean = this.writeTask.commit

  /*
    Path query and expand of node.
    TODO: length
   */
  def paths(startNodeFilter: NodeFilter,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection,
            length: (Int, Int)): Iterator[PathTriple] =
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter {
      case PathTriple(startNode, rel, endNode, _) =>
        relationshipFilter.matches(rel) && startNodeFilter.matches(startNode) && endNodeFilter.matches(endNode)
    }

  def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter(_.startNode.id == nodeId)
  }

  def expand(nodeId: LynxId,
             relationshipFilter: RelationshipFilter,
             endNodeFilter: NodeFilter,
             direction: SemanticDirection ): Iterator[PathTriple] =
    expand(nodeId, direction).filter { pathTriple =>
      relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(pathTriple.endNode)
    }

  /*
    Operations of indexes
   */

  def createIndex(labelName: String, properties: Set[String]): Unit =
    this.indexManager.createIndex(Index(toLabel(labelName), properties.map(toPropertyKey)))

  def dropIndex(labelName: String, properties: Set[String]): Unit =
    this.indexManager.dropIndex(Index(toLabel(labelName), properties.map(toPropertyKey)))

  def indexes: Array[(String, Set[String])] =
    this.indexManager.indexes.map{ case Index(label, properties) => (label.name, properties.map(_.name))}

  /*
    Operations of estimating count.
   */
  def estimateNodeLabel(labelName: String): Long =
    this.statistics.nodeNumberOfLabel(toLabel(labelName))

  def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Long =
    this.statistics.nodeNumberOfProperty(toLabel(labelName), toPropertyKey(propertyName), LynxValue(value))

  def estimateRelationship(relType: String): Long =
    this.statistics.relationshipNumberOfType(toType(relType))

}

trait TreeNode {
  type SerialType <: TreeNode
  val children: Seq[SerialType] = Seq.empty

  def pretty: String = {
    val lines = new mutable.ArrayBuffer[String]

    @tailrec
    def recTreeToString(toPrint: List[TreeNode], prefix: String, stack: List[List[TreeNode]]): Unit = {
      toPrint match {
        case Nil =>
          stack match {
            case Nil =>
            case top :: remainingStack =>
              recTreeToString(top, prefix.dropRight(4), remainingStack)
          }
        case last :: Nil =>
          lines += s"$prefix╙──${last.toString}"
          recTreeToString(last.children.toList, s"$prefix    ", Nil :: stack)
        case next :: siblings =>
          lines += s"$prefix╟──${next.toString}"
          recTreeToString(next.children.toList, s"$prefix║   ", siblings :: stack)
      }
    }

    recTreeToString(List(this), "", Nil)
    lines.mkString("\n")
  }
}

trait LynxException extends RuntimeException {
}

case class ParsingException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class ConstrainViolatedException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class ProcedureUnregisteredException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class NoIndexManagerException(msg: String) extends LynxException {
  override def getMessage: String = msg
}