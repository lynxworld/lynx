package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

case class CypherRunnerContext(dataFrameOperator: DataFrameOperator, expressionEvaluator: ExpressionEvaluator, graphModel: GraphModel)

class CypherRunner(graphModel: GraphModel) extends LazyLogging {
  protected val expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluatorImpl()
  protected val dataFrameOperator: DataFrameOperator = new DataFrameOperatorImpl(expressionEvaluator)
  private implicit lazy val runnerContext = CypherRunnerContext(dataFrameOperator, expressionEvaluator, graphModel)
  protected val logicalPlanner: LogicalPlanner = new LogicalPlannerImpl()(runnerContext)
  protected val physicalPlanner: PhysicalPlanner = new PhysicalPlannerImpl()(runnerContext)
  protected val queryParser: QueryParser = new CachedQueryParser(new QueryParserImpl())

  def compile(query: String): (Statement, Map[String, Any], SemanticState) = queryParser.parse(query)

  def run(query: String, param: Map[String, Any]): LynxResult = {
    val (statement, param2, state) = queryParser.parse(query)
    logger.debug(s"AST tree: ${statement}")

    val logicalPlan = logicalPlanner.plan(statement)
    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    val physicalPlan = physicalPlanner.plan(logicalPlan)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val ctx = PlanExecutionContext(param ++ param2)
    val df = physicalPlan.execute(ctx)

    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames,
          df.records.take(limit).toSeq.map(_.map(_.value)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[Map[String, Any]] = df.records.map(columnNames.zip(_).toMap)

      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)

      override def getLogicalPlan(): LogicalPlanNode = logicalPlan

      override def getPhysicalPlan(): PhysicalPlanNode = physicalPlan

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

case class PlanExecutionContext(queryParameters: Map[String, Any]) {
  val expressionContext = ExpressionContext(queryParameters.map(x => x._1 -> LynxValue(x._2)))
}

trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[Map[String, Any]]
}

trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LogicalPlanNode

  def getPhysicalPlan(): PhysicalPlanNode
}

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]

  def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]]
}

case class NodeFilter(labels: Seq[String], properties: Map[String, LynxValue]) {
  def matches(node: LynxNode): Boolean = (labels, node.labels) match {
    case (Seq(), _) => properties.forall(p => node.property(p._1).orNull.equals(p._2))
    case (_, nodeLabels) => labels.forall(nodeLabels.contains(_)) && properties.forall(p => node.property(p._1).orNull.equals(p._2))
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
  def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] = None

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

  def paths(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    val rels = direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }

    rels.filter(_.startNode.id == nodeId)
  }

  def paths(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    paths(nodeId, direction).filter(
      item => {
        val PathTriple(_, rel, endNode, _) = item
        relationshipFilter.matches(rel) && endNodeFilter.matches(endNode)
      }
    )
  }

  def paths(nodeFilter: NodeFilter, expandFilters: (RelationshipFilter, NodeFilter, SemanticDirection)*): Iterator[Seq[PathTriple]] = {
    expandFilters.drop(1).foldLeft {
      val (relationshipFilter, endNodeFilter, direction) = expandFilters.head
      paths(nodeFilter, relationshipFilter, endNodeFilter, direction).map(Seq(_))
    } {
      (in: Iterator[Seq[PathTriple]], newFilter) =>
        in.flatMap {
          record0: Seq[PathTriple] =>
            val (relationshipFilter, endNodeFilter, direction) = newFilter
            paths(record0.last.endNode.id, relationshipFilter, endNodeFilter, direction).map(
              record0 :+ _).filter(
              item => {
                //(m)-[r]-(n)-[p]-(t), r!=p
                val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
                relIds.size == relIds.toSet.size
              }
            )
        }
    }
  }

  def createElements[T](
    nodesInput: Seq[(String, NodeInput)],
    relsInput: Seq[(String, RelationshipInput)],
    onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T

  def nodes(): Iterator[LynxNode]

  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches(_))
}

trait LynxException extends RuntimeException {
}

case class ParsingException(msg: String) extends LynxException {
  override def getMessage: String = msg
}