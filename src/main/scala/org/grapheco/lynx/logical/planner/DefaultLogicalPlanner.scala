package org.grapheco.lynx.logical.planner

import org.grapheco.lynx._
import org.grapheco.lynx.logical.plans.{LogicalCreateIndex, LogicalDropIndex, LogicalPlan}
import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.translators.QueryPartTranslator
import org.grapheco.lynx.runner.CypherRunnerContext
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
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LogicalPlan = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        QueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(Variable(v1), LabelName(l), List(Property(Variable(v2), PropertyKeyName(p)))) =>
        throw logical.UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) => LogicalCreateIndex(labelName.name, properties.map(_.name))

      case DropIndex(labelName, properties) => LogicalDropIndex(labelName.name, properties.map(_.name))

      case _ =>
        throw logical.UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalPlan = {
    translate(statement)(plannerContext)
  }
}
