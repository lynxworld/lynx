package org.grapheco.lynx

import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.util.symbols.CypherType

trait CypherRunner {
  type NodeIdType = Long
  type RelationIdType = Long

  protected val graphProvider: GraphProvider[NodeIdType, RelationIdType]
  protected val logicalPlanner: LogicalPlanner = new LogicalPlannerImpl
  protected val physicalPlanner: PhysicalPlanner = new PhysicalPlannerImpl
  protected val queryParser: QueryParser = new QueryParserImpl()
  protected val dataFrameOperator: DataFrameOperator = null
  protected val expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluatorImpl

  def cypher(query: String, param: Map[String, Any]): CypherResult = {
    val (statement, param2, state) = queryParser.parse(query)
    val logicalPlan = logicalPlanner.plan(statement, param ++ param2)
    val physicalPlan = physicalPlanner.plan(logicalPlan)
    val ctx = CypherExecutionContext[NodeIdType, RelationIdType](dataFrameOperator, expressionEvaluator, graphProvider)
    val df = physicalPlan.execute[NodeIdType, RelationIdType](ctx)

    new CypherResult() {
      override def show(): Unit =
        FormatUtils.printTable(df.schema.map(_._1), df.records.map(_.map(_.value)).toSeq)

      override def columns(): Seq[(String, CypherType)] = ???

      override def records(): Iterator[Map[String, Any]] = ???
    }
  }
}

case class CypherExecutionContext[NodeIdType, RelationIdType](
                                                               dataFrameOperator: DataFrameOperator,
                                                               expressionEvaluator: ExpressionEvaluator,
                                                               graphProvider: GraphProvider[NodeIdType, RelationIdType]
                                                             )

trait CypherResult {
  def show(): Unit

  def columns(): Seq[(String, CypherType)]

  def records(): Iterator[Map[String, Any]]
}

trait GraphProvider[NodeIdType, RelationIdType] {
  def createElements(nodes: Array[IRNode], rels: Array[IRRelation[NodeIdType, RelationIdType]]): Unit
}

class LynxException(msg: String = null, cause: Throwable = null) extends RuntimeException(msg, cause)

class ParsingException(msg: String) extends LynxException(msg)