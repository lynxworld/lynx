package org.grapheco.lynx.runner

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx._
import org.grapheco.lynx.dataframe.{DataFrameOperator, DefaultDataFrameOperator}
import org.grapheco.lynx.evaluator.{DefaultExpressionEvaluator, ExpressionEvaluator}
import org.grapheco.lynx.logical.planner.{DefaultLogicalPlanner, LogicalPlanner}
import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.plans.LogicalPlan
import org.grapheco.lynx.optimizer.{DefaultPhysicalPlanOptimizer, PhysicalPlanOptimizer}
import org.grapheco.lynx.parser.{CachedQueryParser, DefaultQueryParser, QueryParser}
import org.grapheco.lynx.physical.planner.{DefaultPhysicalPlanner, PhysicalPlanner}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.PhysicalPlan
import org.grapheco.lynx.procedure._
import org.grapheco.lynx.runner.infer.InferEngine
import org.grapheco.lynx.types.{DefaultTypeSystem, TypeSystem}
import org.grapheco.lynx.util.FormatUtils
import org.grapheco.lynx.util.FormatUtils.convertPatternComprehension
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState

import java.util

/**
 * @ClassName CypherRunner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class CypherRunner(var graphModel: GraphModel) extends LazyLogging {

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
    classOf[TrigonometricFunctions],
    classOf[SpatialFunctions],
    classOf[FullTextIndexFunctions])

  protected lazy val expressionEvaluator: ExpressionEvaluator = new DefaultExpressionEvaluator(graphModel, types, procedures)
  protected lazy val dataFrameOperator: DataFrameOperator = new DefaultDataFrameOperator(expressionEvaluator)
   implicit lazy val runnerContext = CypherRunnerContext(types, procedures, dataFrameOperator, expressionEvaluator, graphModel)
  protected lazy val logicalPlanner: LogicalPlanner = new DefaultLogicalPlanner(runnerContext)
  protected lazy val physicalPlanner: PhysicalPlanner = new DefaultPhysicalPlanner(runnerContext)
  protected lazy val physicalPlanOptimizer: PhysicalPlanOptimizer = PhysicalPlanOptimizer.none
  protected lazy val queryParser: QueryParser = new CachedQueryParser(new DefaultQueryParser(runnerContext))
  // infer
  protected lazy val inferEngine: InferEngine = InferEngine.remote

  def registerAnnotatedClass(clazz: Class[_]): Unit = procedures.registerAnnotatedClass(clazz)

  def compile(query: String): (Statement, Map[String, Any], SemanticState) = queryParser.parse(query)

  def run(query: String, param: Map[String, Any], profile: Boolean = false): LynxResult = {
    val query2 = convertPatternComprehension(query)
    val (statement, param2, state) = queryParser.parse(query2)
    logger.info(s"AST tree: ${statement}")

    val logicalPlannerContext = LogicalPlannerContext(param ++ param2, runnerContext)
    val logicalPlan = logicalPlanner.plan(statement, logicalPlannerContext)
    logger.info(s"logical plan: \r\n${logicalPlan.pretty}")

    val physicalPlannerContext = PhysicalPlannerContext(param ++ param2, runnerContext)
    val physicalPlan = physicalPlanner.plan(logicalPlan)(physicalPlannerContext)
    if (!profile) logger.info(s"physical plan: \r\n${physicalPlan.pretty}")

//    val optimizedPhysicalPlan = physicalPlanOptimizer.optimize(physicalPlan, physicalPlannerContext)
//    logger.info(s"optimized physical plan: \r\n${optimizedPhysicalPlan.pretty}")
    val optimizedPhysicalPlan = physicalPlan

    if (profile) {
      val stack = new util.Stack[PhysicalPlan]()
      stack.push(optimizedPhysicalPlan)
      while (!stack.isEmpty) {
        val p = stack.pop()
        p.profileMode = true
        p.children.foreach(stack.push)
      }
    }

    val ctx = ExecutionContext(physicalPlannerContext, statement, param ++ param2, inferEngine = inferEngine)
    val df = optimizedPhysicalPlan.execute(ctx)
    graphModel.write.commit

    if (profile) logger.info(s"profile plan: \r\n${optimizedPhysicalPlan.pretty}")


    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)
      val columnMap = columns().zipWithIndex.toMap

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames, df.records.take(limit).toSeq.map(_.map(types.format)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[LynxRecord] = df.records.map(values=> LynxRecord(columnMap, values))

      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)

      override def getLogicalPlan(): LogicalPlan = logicalPlan

      override def getPhysicalPlan(): PhysicalPlan = physicalPlan

      override def getOptimizerPlan(): PhysicalPlan = optimizedPhysicalPlan

      override def asString(): String = {
        FormatUtils.resultAsString(columnNames, df.records.toSeq.map(_.map(types.format)))
      }

      override def cache(): LynxResult = {
        val source = this
        val cached = df.records.toSeq

        new LynxResult {
          override def show(limit: Int): Unit =
            FormatUtils.printTable(columnNames, cached.take(limit).map(_.map(types.format)))

          override def cache(): LynxResult = this

          override def columns(): Seq[String] = columnNames

          override def records(): Iterator[LynxRecord] = cached.map(values=> LynxRecord(columnMap, values)).toIterator

          override def asString(): String =
            FormatUtils.resultAsString(columnNames, cached.map(_.map(types.format)))
        }
      }
    }
  }
}
