package org.grapheco.lynx.physical

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.runner.CypherRunnerContext

import scala.collection.mutable

/**
 * @ClassName PhysicalPlannerContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class PhysicalPlannerContext(parameterTypes: Seq[(String, LynxType)], runnerContext: CypherRunnerContext, var pptContext: mutable.Map[String, Any] = mutable.Map.empty) {
}

object PhysicalPlannerContext {
  def apply(queryParameters: Map[String, Any], runnerContext: CypherRunnerContext): PhysicalPlannerContext =
    new PhysicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext)
}