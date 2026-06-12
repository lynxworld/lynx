package org.grapheco.lynx.runner

import org.grapheco.lynx.dataframe.DataFrameOperator
import org.grapheco.lynx.evaluator.ExpressionEvaluator
import org.grapheco.lynx.procedure.ProcedureRegistry
import org.grapheco.lynx.types.TypeSystem

/**
 * @ClassName CypherRunnerContext
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class CypherRunnerContext(typeSystem: TypeSystem,
                               procedureRegistry: ProcedureRegistry,
                               dataFrameOperator: DataFrameOperator,
                               expressionEvaluator: ExpressionEvaluator,
                               graphModel: GraphModel)
