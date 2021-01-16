package org.grapheco.lynx

import org.opencypher.v9_0.ast.{Clause, Create, Match, PeriodicCommitHint, Query, QueryPart, Return, SingleQuery, Statement, UnresolvedCall, With}
import org.opencypher.v9_0.util.ASTNode

trait LogicalPlanNode {
}

trait LogicalPlanner {
  def plan(statement: Statement): LogicalPlanNode
}

class LogicalPlannerImpl()(implicit runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translateQueryPart(part: QueryPart): LogicalQueryPart = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        LogicalSingleQuery(
          clauses.foldLeft[Option[LogicalQueryClause]](None) { (source, clause) =>
            //TODO: multiple match
            clause match {
              case c: UnresolvedCall => Some(LogicalProcedureCall(c))
              case r: Return => Some(LogicalReturn(r, source))
              case w: With => Some(LogicalWith(w, source))
              case m: Match => Some(LogicalMatch(m, source))
              case c: Create => Some(LogicalCreate(c, source))
            }
          }
        )
    }
  }

  private def translate(node: ASTNode): LogicalPlanNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LogicalQuery(translateQueryPart(part))

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement): LogicalPlanNode = translate(statement)
}

case class LogicalQuery(part: LogicalQueryPart) extends LogicalPlanNode {

}

trait LogicalQueryPart extends LogicalPlanNode

case class LogicalSingleQuery(source: Option[LogicalQueryClause]) extends LogicalQueryPart {

}

trait LogicalQueryClause extends LogicalPlanNode

case class LogicalWith(w: With, in: Option[LogicalQueryClause]) extends LogicalQueryClause

case class LogicalProcedureCall(c: UnresolvedCall) extends LogicalQueryClause

case class LogicalReturn(r: Return, in: Option[LogicalQueryClause]) extends LogicalQueryClause

case class LogicalMatch(m: Match, in: Option[LogicalQueryClause]) extends LogicalQueryClause

case class LogicalCreate(c: Create, in: Option[LogicalQueryClause]) extends LogicalQueryClause

case class UnknownASTNodeException(node: ASTNode) extends LynxException