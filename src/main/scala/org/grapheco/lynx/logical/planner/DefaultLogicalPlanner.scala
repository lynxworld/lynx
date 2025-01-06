package org.grapheco.lynx.logical.planner

import org.grapheco.lynx.logical._
import org.grapheco.lynx.logical.planner.LogicalPlannerContext
import org.grapheco.lynx.logical.plan._

/**
 * DefaultLogicalPlanner is responsible for translating AST nodes into logical plans.
 */
class DefaultLogicalPlanner extends LogicalPlanner {
  
  /**
   * Translates a given statement into a logical plan.
   * @param statement The statement to translate.
   * @param plannerContext The context for planning.
   * @return The logical plan.
   */
  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalPlan = {
    translate(statement)(plannerContext)
  }

  /**
   * Translates a given AST node into a logical plan.
   * @param node The AST node to translate.
   * @param plannerContext The context for planning.
   * @return The logical plan.
   */
  private def translate(node: ASTNode)(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    node match {
      case CreateIndex(labelName, properties) => 
        LogicalCreateIndex(labelName.name, properties.map(_.name))

      case DropIndex(labelName, properties) => 
        LogicalDropIndex(labelName.name, properties.map(_.name))

      case _ =>
        throw logical.UnknownASTNodeException(node)
    }
  }
}
