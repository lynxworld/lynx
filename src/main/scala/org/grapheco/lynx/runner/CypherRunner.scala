package org.grapheco.lynx.runner

import com.typesafe.scalalogging.LazyLogging

import org.grapheco.lynx.dataframe.{DataFrameOperator, DefaultDataFrameOperator}
import org.grapheco.lynx.evaluator.{DefaultExpressionEvaluator, ExpressionEvaluator}
import org.grapheco.lynx.logical.{DefaultLogicalPlanner, LPTNode, LogicalPlanner, LogicalPlannerContext}
import org.grapheco.lynx.parser.{CachedQueryParser, DefaultQueryParser, QueryParser}
import org.grapheco.lynx.procedure._
import org.grapheco.lynx.types.{DefaultTypeSystem, LynxValue, TypeSystem}
import org.grapheco.lynx.util.FormatUtils
import org.grapheco.lynx._
import org.grapheco.lynx.optimizer.{DefaultPhysicalPlanOptimizer, PhysicalPlanOptimizer}
import org.grapheco.lynx.physical.{DefaultPhysicalPlanner, PPTNode, PhysicalPlanner, PhysicalPlannerContext}
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState

/**
 * @ClassName CypherRunner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class CypherRunner(graphModel: GraphModel) extends LazyLogging {
  protected lazy val types: TypeSystem = new DefaultTypeSystem()
  val scalarFunctions: ScalarFunctions = new ScalarFunctions(graphModel)
  protected lazy val procedures: WithGraphModelProcedureRegistry = new WithGraphModelProcedureRegistry(types,
    scalarFunctions,
    classOf[AggregatingFunctions],
    classOf[ListFunctions],
    classOf[LogarithmicFunctions],
    classOf[NumericFunctions],
    classOf[PredicateFunctions],
    classOf[StringFunctions],
    classOf[TimeFunctions],
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
