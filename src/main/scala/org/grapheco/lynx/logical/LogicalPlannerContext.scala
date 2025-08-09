package org.grapheco.lynx.logical

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.runner.CypherRunnerContext

/**
 * @ClassName LogicalPlannerContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
object LogicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): LogicalPlannerContext =
    new LogicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      Seq.empty,
      runnerContext)
}

case class LogicalPlannerContext(parameterTypes: Seq[(String, LynxType)],
                                 var variables: Seq[(String, LynxType)],
                                 runnerContext: CypherRunnerContext)