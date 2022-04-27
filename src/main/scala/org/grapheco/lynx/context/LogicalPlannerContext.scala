package org.grapheco.lynx.context

import org.grapheco.lynx.LynxType

/**
 * @ClassName LogicalPlannerContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
//TODO: LogicalPlannerContext vs. PhysicalPlannerContext?
object LogicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): LogicalPlannerContext =
    new LogicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext)
}

case class LogicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext)