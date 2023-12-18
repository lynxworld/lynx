package org.grapheco.lynx.runner

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.ast.Statement

/**
 * @ClassName ExecutionContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
//TODO: context.context??
case class ExecutionContext(physicalPlannerContext: PhysicalPlannerContext, statement: Statement, queryParameters: Map[String, Any], val arguments: DataFrame = DataFrame.empty) {
  val expressionContext = ExpressionContext(this, queryParameters.map(x => x._1 -> physicalPlannerContext.runnerContext.typeSystem.wrap(x._2)))
  def withArguments(df: DataFrame): ExecutionContext = ExecutionContext(physicalPlannerContext, statement, queryParameters, df)
}
