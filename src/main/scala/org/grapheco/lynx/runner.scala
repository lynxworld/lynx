
package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.procedure.functions.{AggregatingFunctions, ListFunctions, LogarithmicFunctions, NumericFunctions, PredicateFunctions, ScalarFunctions, StringFunctions, TimeFunctions, TrigonometricFunctions}
import org.grapheco.lynx.procedure.{DefaultProcedureRegistry, ProcedureRegistry}
import org.grapheco.lynx.util.FormatUtils
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.types.{DefaultTypeSystem, LynxValue, TypeSystem}
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
  protected lazy val procedures: DefaultProcedureRegistry = new DefaultProcedureRegistry(types,
    classOf[AggregatingFunctions],
    classOf[ListFunctions],
    classOf[LogarithmicFunctions],
    classOf[NumericFunctions],
    classOf[PredicateFunctions],
    classOf[ScalarFunctions],
    classOf[StringFunctions],
//    classOf[TimeFunctions],
    classOf[TrigonometricFunctions])
  protected lazy val expressionEvaluator: ExpressionEvaluator = new DefaultExpressionEvaluator(graphModel, types, procedures)
  protected lazy val dataFrameOperator: DataFrameOperator = new DefaultDataFrameOperator(expressionEvaluator)
  private implicit lazy val runnerContext = CypherRunnerContext(types, procedures, dataFrameOperator, expressionEvaluator, graphModel)
  protected lazy val logicalPlanner: LogicalPlanner = new DefaultLogicalPlanner(runnerContext)
  protected lazy val physicalPlanner: PhysicalPlanner = new DefaultPhysicalPlanner(runnerContext)
  protected lazy val physicalPlanOptimizer: PhysicalPlanOptimizer = new DefaultPhysicalPlanOptimizer(runnerContext)
  protected lazy val queryParser: QueryParser = new CachedQueryParser(new DefaultQueryParser(runnerContext))

  def compile(query: String): (Statement, Map[String, Any], SemanticState) = queryParser.parse(query)

  def run(query: String, param: Map[String, Any]): LynxResult = {
    val (statement, param2, state) = queryParser.parse(query)
    logger.debug(s"AST tree: ${statement}")

    val logicalPlannerContext = LogicalPlannerContext(param ++ param2, runnerContext)
    val logicalPlan = logicalPlanner.plan(statement, logicalPlannerContext)
    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    val physicalPlannerContext = PhysicalPlannerContext(param ++ param2, runnerContext)
    val physicalPlan = physicalPlanner.plan(logicalPlan)(physicalPlannerContext)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val optimizedPhysicalPlan = physicalPlanOptimizer.optimize(physicalPlan, physicalPlannerContext)
    logger.debug(s"optimized physical plan: \r\n${optimizedPhysicalPlan.pretty}")

    val ctx = ExecutionContext(physicalPlannerContext, statement, param ++ param2)
    val df = optimizedPhysicalPlan.execute(ctx)
    graphModel.write.commit


    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames,
          df.records.take(limit).toSeq.map(_.map(_.value)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[Map[String, LynxValue]] = df.records.map(columnNames.zip(_).toMap)

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

          override def records(): Iterator[Map[String, LynxValue]] = cached.map(columnNames.zip(_).toMap).iterator

        }
      }
    }
  }

}

//TODO: LogicalPlannerContext vs. PhysicalPlannerContext?
object LogicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): LogicalPlannerContext =
    new LogicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext)
}

case class LogicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext)

object PhysicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): PhysicalPlannerContext =
    new PhysicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext)
}

case class PhysicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext, var pptContext: mutable.Map[String, Any]=mutable.Map.empty) {
}

//TODO: context.context??
case class ExecutionContext(physicalPlannerContext: PhysicalPlannerContext, statement: Statement, queryParameters: Map[String, Any]) {
  val expressionContext = ExpressionContext(this, queryParameters.map(x => x._1 -> physicalPlannerContext.runnerContext.typeSystem.wrap(x._2)))
}

trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[Map[String, LynxValue]]
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
case class NodeFilter(labels: Seq[LynxNodeLabel], properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(node: LynxNode): Boolean = labels.forall(node.labels.contains) &&
    properties.forall { case (propertyName, value) => node.property(propertyName).exists(value.equals) }
}

/**
 * types note: the relationship of type TYPE1 or of type TYPE2.
 * @param types type names
 * @param properties filter property names
 */
case class RelationshipFilter(types: Seq[LynxRelationshipType], properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(relationship: LynxRelationship): Boolean = ((types, relationship.relationType) match {
    case (Seq(), _) => true
    case (_, None) => false
    case (_, Some(typeName)) => types.contains(typeName)
  }) && properties.forall { case (propertyName, value) => relationship.property(propertyName).exists(value.equals) }
}

/**
 * A triplet of path.
 * @param startNode start node
 * @param storedRelation the relation from start-node to end-node
 * @param endNode end node
 * @param reverse If true, it means it is in reverse order
 */
case class PathTriple(startNode: LynxNode, storedRelation: LynxRelationship, endNode: LynxNode, reverse: Boolean = false) {
  def revert: PathTriple = PathTriple(endNode, storedRelation, startNode, !reverse)
}

/**
 * Recording the number of nodes and relationships under various conditions for optimization.
 */
trait Statistics {

  /**
   * Count the number of nodes.
   * @return The number of nodes
   */
  def numNode: Long

  /**
   * Count the number of nodes containing specified label.
   * @param labelName The label to contained
   * @return The number of nodes meeting conditions
   */
  def numNodeByLabel(labelName: LynxNodeLabel): Long

  /**
   * Count the number of nodes containing specified label and property.
   * @param labelName The label to contained
   * @param propertyName The property to contained
   * @param value The value of this property
   * @return The number of nodes meeting conditions
   */
  def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long

  /**
   * Count the number of relationships.
   * @return The number of relationships
   */
  def numRelationship: Long

  /**
   * Count the number of relationships have specified type.
   * @param typeName The type
   * @return The number of relationships meeting conditions.
   */
  def numRelationshipByType(typeName: LynxRelationshipType): Long
}

/**
 * An index consists of a label and several properties.
 * eg: Index("Person", Set("name"))
 * @param labelName The label
 * @param properties The properties, Non repeatable.
 */
case class Index(labelName: LynxNodeLabel, properties: Set[LynxPropertyKey])

/**
 * Manage index creation and deletion.
 */
trait IndexManager {

  /**
   * Create Index.
   * @param index The index to create
   */
  def createIndex(index: Index): Unit

  /**
   * Drop Index.
   * @param index The index to drop
   */
  def dropIndex(index: Index): Unit

  /**
   * Get all indexes.
   * @return all indexes
   */
  def indexes: Array[Index]
}

/**
 * Create and delete of nodes and relationships.
 * Write operations on label, type and property.
 */
trait WriteTask {

  /**
   * Create elements, including nodes and relationships.
   * @param nodesInput A series of nodes to be created. (the name of this node, the value of this node)
   * @param relationshipsInput A series of relationships to be created. (the name of this relationship, the value of this relationship)
   * @param onCreated A callback function that needs input the created nodes and relationships
   * @tparam T Return type of onCreated
   * @return Return value of onCreated
   */
  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T

  /**
   * Delete relations.
   * @param ids the ids of relations to deleted
   */
  def deleteRelations(ids: Iterator[LynxId]): Unit

  /**
   * Delete nodes. No need to consider whether the node has relationships.
   * @param ids the ids of nodes to deleted
   */
  def deleteNodes(ids: Seq[LynxId]): Unit

  /**
   * Set properties of nodes.
   * @param nodeIds The ids of nodes to modified
   * @param data An array of key value pairs that represent the properties and values to be modified
   * @param cleanExistProperties If it is true, properties will be overwritten; otherwise, properties will be modified
   * @return These nodes after modifying the properties
   */
  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey ,Any)], cleanExistProperties: Boolean = false): Iterator[Option[LynxNode]]

  /**
   * Add labels to nodes.
   * @param nodeIds The ids of nodes to modified
   * @param labels The labels to added
   * @return These nodes after adding labels
   */
  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]]

  /**
   * Set properties of relationships.
   * @param relationshipIds The ids of relationships to modified
   * @param data An array of key value pairs that represent the properties and values to be modified
   * @return These relationships after modifying the properties
   */
  def setRelationshipsProperties(relationshipIds: Iterator[LynxId],  data: Array[(LynxPropertyKey ,Any)]): Iterator[Option[LynxRelationship]]

  /**
   * Set type of relationships.
   * @param relationshipIds The ids of relationships to modified
   * @param typeName The type to set
   * @return These relationships after set the type
   */
  def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]]

  /**
   * Remove properties of nodes.
   * @param nodeIds The ids of nodes to modified
   * @param data An array of properties to be removed
   * @return These nodes after modifying the properties
   */
  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]]

  /**
   * Remove labels from nodes.
   * @param nodeIds The ids of nodes to modified
   * @param labels The labels to removed
   * @return These nodes after removing labels
   */
  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]]

  /**
   * Remove properties of relationships.
   * @param relationshipIds The ids of relationships to modified
   * @param data An array of properties to be removed
   * @return These relationships after modifying the properties
   */
  def removeRelationshipsProperties(relationshipIds: Iterator[LynxId],  data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]]

  /**
   * Remove type of relationships.
   * @param relationshipIds The ids of relationships to modified
   * @param typeName The type to removed
   * @return These relationships after removing the type
   */
  def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]]

  /**
   * Commit write tasks. It is called at the end of the statement.
   * @return Is it successful?
   */
  def commit: Boolean

}


trait GraphModel{

  /**
   * A Statistics object needs to be returned.
   * In the default implementation, those number is obtained through traversal and filtering.
   * You can override the default implementation.
   * @return The Statistics object
   */
  def statistics: Statistics = new Statistics {
    override def numNode: Long = nodes().length

    override def numNodeByLabel(labelName: LynxNodeLabel): Long = nodes(NodeFilter(Seq(labelName), Map.empty)).length

    override def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long =
      nodes(NodeFilter(Seq(labelName), Map(propertyName->value))).length

    override def numRelationship: Long = relationships().length

    override def numRelationshipByType(typeName: LynxRelationshipType): Long =
      relationships(RelationshipFilter(Seq(typeName), Map.empty)).length
  }

  /**
   * An IndexManager object needs to be returned.
   * In the default implementation, the returned indexes is empty,
   * and the addition and deletion of any index will throw an exception.
   * You need override the default implementation.
   * @return The IndexManager object
   */
  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index creation")

    override def dropIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index dropping")

    override def indexes: Array[Index] = Array.empty
  }

  /**
   * An WriteTask object needs to be returned.
   * There is no default implementation, you must override it.
   * @return The WriteTask object
   */
  def write: WriteTask

  /**
   * All nodes.
   * @return An Iterator of all nodes.
   */
  def nodes(): Iterator[LynxNode]

  /**
   * All nodes with a filter.
   * @param nodeFilter The filter
   * @return An Iterator of all nodes after filter.
   */
  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches)

  /**
   * Return all relationships as PathTriple.
   * @return An Iterator of PathTriple
   */
  def relationships(): Iterator[PathTriple]

  /**
   * Return all relationships as PathTriple with a filter.
   * @param relationshipFilter The filter
   * @return An Iterator of PathTriple after filter
   */
  def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = relationships().filter(f => relationshipFilter.matches(f.storedRelation))


  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T =
    this.write.createElements(nodesInput, relationshipsInput, onCreated)

  def deleteRelations(ids: Iterator[LynxId]): Unit = this.write.deleteRelations(ids)

  def deleteNodes(ids: Seq[LynxId]): Unit = this.write.deleteNodes(ids)

  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(String ,Any)], cleanExistProperties: Boolean = false): Iterator[Option[LynxNode]]  =
    this.write.setNodesProperties(nodeIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)), cleanExistProperties)

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]]  =
    this.write.setNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def setRelationshipsProperties(relationshipIds: Iterator[LynxId],  data: Array[(String ,Any)]): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsProperties(relationshipIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)))

  def setRelationshipsType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[String]): Iterator[Option[LynxNode]]  =
    this.write.removeNodesProperties(nodeIds, data.map(LynxPropertyKey))

  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.removeNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def removeRelationshipsProperties(relationshipIds: Iterator[LynxId],  data: Array[String]): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsProperties(relationshipIds, data.map(LynxPropertyKey))

  def removeRelationshipType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  /**
   * Delete nodes in a safe way, and handle nodes with relationships in a special way.
   * @param nodesIDs The ids of nodes to deleted
   * @param forced When some nodes have relationships,
   *               if it is true, delete any related relationships,
   *               otherwise throw an exception
   */
  def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = relationships().map(_.storedRelation)
      .filter(rel => ids.contains(rel.startNodeId) || ids.contains(rel.endNodeId))
    if (affectedRelationships.nonEmpty) {
      if (forced)
        this.write.deleteRelations(affectedRelationships.map(_.id))
      else
        throw ConstrainViolatedException(s"deleting referred nodes")
    }
    this.write.deleteNodes(ids.toSeq)
  }

  def commit(): Boolean = this.write.commit

  /**
   * Get the paths that meets the conditions
   * @param startNodeFilter Filter condition of starting node
   * @param relationshipFilter Filter conditions for relationships
   * @param endNodeFilter Filter condition of ending node
   * @param direction Direction of relationships, INCOMING, OUTGOING or BOTH
   * @param upperLimit Upper limit of relationship length
   * @param lowerLimit Lower limit of relationship length
   * @return The paths
   */
  def paths(startNodeFilter: NodeFilter,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection,
            upperLimit: Option[Int],
            lowerLimit: Option[Int]): Iterator[PathTriple] =
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter {
      case PathTriple(startNode, rel, endNode, _) =>
        relationshipFilter.matches(rel) && startNodeFilter.matches(startNode) && endNodeFilter.matches(endNode)
    }

  /**
   * Take a node as the starting or ending node and expand in a certain direction.
   * @param nodeId The id of this node
   * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion
   */
  def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter(_.startNode.id == nodeId)
  }

  /**
   * Take a node as the starting or ending node and expand in a certain direction with some filter.
   * @param nodeId The id of this node
   * @param relationshipFilter conditions for relationships
   * @param endNodeFilter Filter condition of ending node
   * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion and filter
   */
  def expand(nodeId: LynxId,
             relationshipFilter: RelationshipFilter,
             endNodeFilter: NodeFilter,
             direction: SemanticDirection ): Iterator[PathTriple] =
    expand(nodeId, direction).filter { pathTriple =>
      relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(pathTriple.endNode)
    }

  /**
   * GraphHelper
   */
  val _helper: GraphModelHelper = GraphModelHelper(this)
}

case class GraphModelHelper(graphModel: GraphModel) {
  /*
    Operations of indexes
   */
  def createIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager.createIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def dropIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager.dropIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def indexes: Array[(String, Set[String])] =
    this.graphModel.indexManager.indexes.map{ case Index(label, properties) => (label.value, properties.map(_.value))}

  /*
    Operations of estimating count.
   */
  def estimateNodeLabel(labelName: String): Long =
    this.graphModel.statistics.numNodeByLabel(LynxNodeLabel(labelName))

  def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Long =
    this.graphModel.statistics.numNodeByProperty(LynxNodeLabel(labelName),LynxPropertyKey(propertyName), LynxValue(value))

  def estimateRelationship(relType: String): Long =
    this.graphModel.statistics.numRelationshipByType(LynxRelationshipType(relType))
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