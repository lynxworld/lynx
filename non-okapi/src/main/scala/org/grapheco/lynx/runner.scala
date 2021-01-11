package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.expressions.{LabelName}

case class CypherRunnerContext(dataFrameOperator: DataFrameOperator, expressionEvaluator: ExpressionEvaluator, graphModel: GraphModel)

class CypherRunner(graphModel: GraphModel) extends LazyLogging {
  protected val expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluatorImpl()
  protected val dataFrameOperator: DataFrameOperator = new DataFrameOperatorImpl(expressionEvaluator)
  private implicit lazy val runnerContext = CypherRunnerContext(dataFrameOperator, expressionEvaluator, graphModel)
  protected val logicalPlanner: LogicalPlanner = new LogicalPlannerImpl()(runnerContext)
  protected val physicalPlanner: PhysicalPlanner = new PhysicalPlannerImpl()(runnerContext)
  protected val queryParser: QueryParser = new CachedQueryParser(new QueryParserImpl())

  def compile(query: String): (Statement, Map[String, Any], SemanticState) = queryParser.parse(query)

  def cypher(query: String, param: Map[String, Any]): CypherResult = {
    val (statement, param2, state) = queryParser.parse(query)
    logger.debug(s"AST tree: ${statement}")

    val logicalPlan = logicalPlanner.plan(statement)
    logger.debug(s"logical plan: ${logicalPlanner}")

    val physicalPlan = physicalPlanner.plan(logicalPlan)
    logger.debug(s"physical plan: ${physicalPlan}")

    val ctx = PlanExecutionContext(param ++ param2)
    val df = physicalPlan.execute(ctx)

    new CypherResult() with PlanAware {
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
    }
  }
}

case class PlanExecutionContext(queryParameters: Map[String, Any]) {
  val expressionContext = ExpressionContext(queryParameters.map(x => x._1 -> CypherValue(x._2)))
}

trait CypherResult {
  def show(limit: Int = 20): Unit

  def columns(): Seq[String]

  def records(): Iterator[Map[String, Any]]
}

trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LogicalPlanNode

  def getPhysicalPlan(): PhysicalPlanNode
}

trait GraphModel {
  def rels(includeStartNodes: Boolean,
           includeEndNodes: Boolean): Iterator[(CypherRelationship, Option[CypherNode], Option[CypherNode])]

  def rels(types: Seq[String], labels1: Seq[LabelName], labels2: Seq[LabelName],
           includeStartNodes: Boolean,
           includeEndNodes: Boolean): Iterator[(CypherRelationship, Option[CypherNode], Option[CypherNode])] = rels(includeStartNodes, includeEndNodes).filter(item => {
    val (rel, _, _) = item
    rel.relationType.isDefined &&
      types.contains(rel.relationType.get) &&
      (labels1.isEmpty || labels1.find(!nodeAt(rel.startNodeId).get.labels.contains(_)).isEmpty) &&
      (labels2.isEmpty || labels2.find(!nodeAt(rel.endNodeId).get.labels.contains(_)).isEmpty)
  }
  )

  def createElements(nodes: Array[Node2Create], rels: Array[Relationship2Create]): Unit

  def nodes(): Iterator[CypherNode]

  def nodeAt(id: CypherId): Option[CypherNode]

  def nodes(labels: Seq[String], exact: Boolean): Iterator[CypherNode] = nodes().filter(node =>
    if (exact) {
      node.labels.diff(labels).isEmpty
    }
    else {
      labels.find(!node.labels.contains(_)).isEmpty
    }
  )
}

class LynxException(msg: String = null, cause: Throwable = null) extends RuntimeException(msg, cause)

class ParsingException(msg: String) extends LynxException(msg)