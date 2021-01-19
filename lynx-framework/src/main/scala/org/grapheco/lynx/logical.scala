package org.grapheco.lynx

import org.opencypher.v9_0.ast.{Clause, Create, Match, PeriodicCommitHint, Query, QueryPart, Return, SingleQuery, Statement, UnresolvedCall, With}
import org.opencypher.v9_0.util.ASTNode
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

trait TreeNode {

  val children: Seq[TreeNode] = Seq.empty

  def pretty: String = {
    val lines = new ArrayBuffer[String]

    @tailrec
    def recTreeToString(toPrint: List[TreeNode], prefix: String, stack: List[List[TreeNode]]): Unit = {
      toPrint match {
        case Nil =>
          stack match {
            case Nil =>
            case top :: remainingStack =>
              recTreeToString(top, prefix.dropRight(4), remainingStack)
          }
        case last :: Nil =>
          lines += s"$prefix╙──${last.toString}"
          recTreeToString(last.children.toList, s"$prefix    ", Nil :: stack)
        case next :: siblings =>
          lines += s"$prefix╟──${next.toString}"
          recTreeToString(next.children.toList, s"$prefix║   ", siblings :: stack)
      }
    }

    recTreeToString(List(this), "", Nil)
    lines.mkString("\n")
  }
}

trait LogicalPlanNode extends TreeNode {
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
  override val children: Seq[TreeNode] = Seq(part)
}

trait LogicalQueryPart extends LogicalPlanNode

case class LogicalSingleQuery(source: Option[LogicalQueryClause]) extends LogicalQueryPart {
  override val children: Seq[TreeNode] = source.toSeq
}

trait LogicalQueryClause extends LogicalPlanNode

case class LogicalWith(w: With, in: Option[LogicalQueryClause]) extends LogicalQueryClause {
  override val children: Seq[TreeNode] = in.toSeq
}

case class LogicalProcedureCall(c: UnresolvedCall) extends LogicalQueryClause

case class LogicalReturn(r: Return, in: Option[LogicalQueryClause]) extends LogicalQueryClause {
  override val children: Seq[TreeNode] = in.toSeq
}

case class LogicalMatch(m: Match, in: Option[LogicalQueryClause]) extends LogicalQueryClause {
  override val children: Seq[TreeNode] = in.toSeq
}

case class LogicalCreate(c: Create, in: Option[LogicalQueryClause]) extends LogicalQueryClause {
  override val children: Seq[TreeNode] = in.toSeq
}

case class UnknownASTNodeException(node: ASTNode) extends LynxException