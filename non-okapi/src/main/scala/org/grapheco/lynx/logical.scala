package org.grapheco.lynx

import org.opencypher.v9_0.ast.{Clause, Create, Match, PeriodicCommitHint, Query, QueryPart, Return, SingleQuery, Statement, With}
import org.opencypher.v9_0.util.ASTNode

trait LogicalPlanNode {
}

trait LogicalPlanner {
  def plan(statement: Statement, map: Map[String, Any]): LogicalPlanNode

}

class LogicalPlannerImpl extends LogicalPlanner {
  private def translateQueryPart(part: QueryPart): LogicalQueryPart = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        LogicalSingleQuery(
          clauses.foldLeft[Option[LogicalQuerySource]](None) { (source, clause) =>
            clause match {
              case r: Return => Some(LogicalReturn(r, source))
              case w: With => Some(LogicalWith(w, source))
              case m: Match => Some(LogicalMatch(m, source))
              case c: Create => Some(LogicalCreate(c, source))
            }
          }
        )
    }
  }

  private def translate(node: ASTNode)(implicit map: Map[String, Any]): LogicalPlanNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LogicalQuery(translateQueryPart(part))

      case _ =>
        throw new RuntimeException(s"unknown element: $node")
    }
  }

  override def plan(statement: Statement, map: Map[String, Any]): LogicalPlanNode = translate(statement)(map)
}

case class LogicalQuery(part: LogicalQueryPart) extends LogicalPlanNode {

}

trait LogicalQueryPart extends LogicalPlanNode

case class LogicalSingleQuery(source: Option[LogicalQuerySource]) extends LogicalQueryPart {

}

trait LogicalQuerySource extends LogicalPlanNode

case class LogicalWith(w: With, in: Option[LogicalQuerySource]) extends LogicalQuerySource

case class LogicalReturn(r: Return, in: Option[LogicalQuerySource]) extends LogicalQuerySource

case class LogicalMatch(m: Match, in: Option[LogicalQuerySource]) extends LogicalQuerySource

case class LogicalCreate(c: Create, in: Option[LogicalQuerySource]) extends LogicalQuerySource

