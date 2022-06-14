package org.grapheco.lynx.evaluator

import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue

/**
 * @ClassName ExpressionContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class ExpressionContext(executionContext: ExecutionContext, params: Map[String, LynxValue], vars: Map[String, LynxValue] = Map.empty) {
  def param(name: String): LynxValue = params(name)

  def var0(name: String): LynxValue = vars(name)

  def withVars(vars0: Map[String, LynxValue]): ExpressionContext = ExpressionContext(executionContext, params, vars0)
}
