package org.grapheco.lynx.logical

import org.grapheco.lynx.context.{CypherRunnerContext, LogicalPlannerContext}
import org.grapheco.lynx.{logical, _}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{LabelName, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.ASTNode

/**
 * @ClassName DefaultLogicalPlanner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LPTNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LPTQueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(Variable(v1), LabelName(l), List(Property(Variable(v2), PropertyKeyName(p)))) =>
        throw logical.UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) =>
        LPTCreateIndex(labelName, properties)

      case _ =>
        throw logical.UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LPTNode = {
    translate(statement)(plannerContext)
  }
}
