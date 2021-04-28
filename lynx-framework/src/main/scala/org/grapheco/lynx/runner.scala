
package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName, SemanticDirection}
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

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
    new LogicalPlannerContext(queryParameters.map(x => x._1 -> runnerContext.typeSystem.wrap(x._2).cypherType).toSeq, runnerContext)
}

case class LogicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext) {
}

object PhysicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): PhysicalPlannerContext =
    new PhysicalPlannerContext(queryParameters.map(x => x._1 -> runnerContext.typeSystem.wrap(x._2).cypherType).toSeq, runnerContext)
}

case class PhysicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext) {
}

//TODO: context.context??
case class ExecutionContext(physicalPlannerContext: PhysicalPlannerContext, statement: Statement, queryParameters: Map[String, Any]) {
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
}

case class NodeFilter(labels: Seq[String], properties: Map[String, LynxValue]) {
  def matches(node: LynxNode): Boolean = (labels, node.labels) match {
    case (Seq(), _) => properties.forall(p => node.property(p._1).orNull.equals(p._2))
    case (_, nodeLabels) => labels.forall(nodeLabels.contains(_)) && properties.forall(p => node.property(p._1).getOrElse(None).equals(p._2))
  }
}

case class RelationshipFilter(types: Seq[String], properties: Map[String, LynxValue]) {
  def matches(rel: LynxRelationship): Boolean = (types, rel.relationType) match {
    case (Seq(), _) => true
    case (_, None) => false
    case (_, Some(relationType)) => types.contains(relationType)
  }
}

case class PathTriple(startNode: LynxNode, storedRelation: LynxRelationship, endNode: LynxNode, reverse: Boolean = false) {
  def revert = PathTriple(endNode, storedRelation, startNode, !reverse)
}

trait GraphModel {
  def relationships(): Iterator[PathTriple]

  def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    val rels = direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }

    rels.filter {
      case PathTriple(startNode, rel, endNode, _) =>
        relationshipFilter.matches(rel) && startNodeFilter.matches(startNode) && endNodeFilter.matches(endNode)
    }
  }

  def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    val rels = direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }

    rels.filter(_.startNode.id == nodeId)
  }

  def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    expand(nodeId, direction).filter(
      item => {
        val PathTriple(_, rel, endNode, _) = item
        relationshipFilter.matches(rel) && endNodeFilter.matches(endNode)
      }
    )
  }

  def createElements[T](
                         nodesInput: Seq[(String, NodeInput)],
                         relsInput: Seq[(String, RelationshipInput)],
                         onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T

  def createIndex(labelName: LabelName, properties: List[PropertyKeyName]): Unit

  def getIndexes(): Array[(LabelName, List[PropertyKeyName])]

  def nodes(): Iterator[LynxNode]

  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches(_))
}

trait TreeNode {
  type SerialType <: TreeNode
  val children: Seq[SerialType] = Seq.empty

  def pretty: String = {
    val lines = new ArrayBuffer[String]

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