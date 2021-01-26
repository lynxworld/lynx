package org.grapheco.lynx

import org.opencypher.v9_0.ast.{Clause, Create, Limit, Match, PeriodicCommitHint, Query, QueryPart, Return, ReturnItem, ReturnItems, ReturnItemsDef, SingleQuery, Skip, Statement, UnresolvedCall, Where, With}
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, NodePattern, Pattern, PatternElement, PatternPart, RelationshipChain, RelationshipPattern}
import org.opencypher.v9_0.util.ASTNode

trait LPTNode extends TreeNode {
}

trait LogicalPlanner {
  def plan(statement: Statement): LPTNode
}

trait LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode
}

case class PipedTranslators(items: Seq[LPTNodeTranslator]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    items.foldLeft[Option[LPTNode]](None) {
      (in, item) => Some(item.translate(in)(lpc))
    }.get
  }
}

class LogicalPlannerImpl()(implicit runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LPTNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LPTQueryPartTranslator(part).translate(None)

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement): LPTNode = {
    val lpc = LogicalPlannerContext()
    translate(statement)(lpc)
  }
}

/////////////////ProcedureCall/////////////
case class LPTProcedureCallTranslator(c: UnresolvedCall) extends LPTNodeTranslator {
  override def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode =
    LPTProcedureCall(c)
}

case class LPTProcedureCall(c: UnresolvedCall) extends LPTNode

//////////////////Create////////////////
case class LPTCreateTranslator(c: Create) extends LPTNodeTranslator {
  override def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode =
    LPTCreate(c)(in)
}

case class LPTCreate(c: Create)(in: Option[LPTNode]) extends LPTNode {
}

///////////////////////////////////////
case class LPTQueryPartTranslator(part: QueryPart) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        PipedTranslators(
          clauses.map(
            _ match {
              case c: UnresolvedCall => LPTProcedureCallTranslator(c)
              case r: Return => LPTSelectTranslator(r.returnItems)
              case w: With => LPTWithTranslator(w)
              case m: Match => LPTMatchTranslator(m)
              case c: Create => LPTCreateTranslator(c)
            }
          )
        ).translate(in)
    }
  }
}

///////////////with,return////////////////
case class LPTSelectTranslator(ri: ReturnItemsDef) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    LPTSelect(ri)(in.get)
  }
}

case class LPTProject(items: Seq[ReturnItem])(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

case class LPTLimit(expression: Expression)(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

case class LPTSkip(expression: Expression)(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

case class LPTFilter(expr: Expression)(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

case class LPTWhereTranslator(where: Option[Where]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    where match {
      case None => in.get
      case Some(Where(expr)) => LPTFilter(expr)(in.get)
    }
  }
}

case class LPTWithTranslator(w: With) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    (w, in) match {
      case (With(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit: Option[Limit], where), None) =>
        LPTCreateUnit(items)

      case (With(distinct, ri: ReturnItems, orderBy, skip: Option[Skip], limit: Option[Limit], where: Option[Where]), Some(sin)) =>
        PipedTranslators(
          Seq(
            LPTProjectTranslator(ri),
            LPTWhereTranslator(where),
            LPTSkipTranslator(skip),
            LPTLimitTranslator(limit),
            LPTSelectTranslator(ri),
            LPTDistinctTranslator(distinct)
          )).translate(in)
    }
  }
}

case class LPTCreateUnit(items: Seq[ReturnItem]) extends LPTNode {
}

case class LPTProjectTranslator(ri: ReturnItems) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    LPTProject(ri.items)(in.get)
  }
}

case class LPTLimitTranslator(limit: Option[Limit]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    limit match {
      case None => in.get
      case Some(Limit(expr)) => LPTLimit(expr)(in.get)
    }
  }
}

case class LPTDistinctTranslator(distinct: Boolean) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    distinct match {
      case false => in.get
      case true => LPTDistinct()(in.get)
    }
  }
}

case class LPTSkipTranslator(skip: Option[Skip]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    skip match {
      case None => in.get
      case Some(Skip(expr)) => LPTSkip(expr)(in.get)
    }
  }
}

case class LPTDistinct()(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

case class LPTSelect(ri: ReturnItemsDef)(in: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(in)
}

///////////////////////////////////////
case class LPTJoin()(a: LPTNode, b: LPTNode) extends LPTNode {
  override val children: Seq[TreeNode] = Seq(a, b)
}

/////////////////match/////////////////
case class LPTNodeScan(np: NodePattern) extends LPTNode {

}

case class LPTRelationshipScan(
  leftNode: NodePattern,
  relationship: RelationshipPattern,
  rightNode: NodePattern) extends LPTNode {
}

case class LPTRelationshipChain(leftScan: LPTRelationshipScan, relationship: RelationshipPattern,
                                rightNode: NodePattern) extends LPTNode {
}

case class LPTMatchTranslator(m: Match) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit lpc: LogicalPlannerContext): LPTNode = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val parts = patternParts.map(matchPatternPart(_)(lpc))
    val matched = (parts.drop(1)).foldLeft(parts.head)((a, b) => LPTJoin()(a, b))

    LPTWhereTranslator(where).translate(Some(matched))
  }

  private def matchPatternPart(patternPart: PatternPart)(implicit lpc: LogicalPlannerContext): LPTNode = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def matchPattern(element: PatternElement)(implicit lpc: LogicalPlannerContext): LPTNode = {
    element match {
      //match (m:label1)
      case np: NodePattern =>
        LPTNodeScan(np)

      //match ()-[]->()
      case rc@RelationshipChain(
      leftNode: NodePattern,
      relationship: RelationshipPattern,
      rightNode: NodePattern) =>
        LPTRelationshipScan(leftNode, relationship, rightNode)

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc@RelationshipChain(
      leftChain: RelationshipChain,
      relationship: RelationshipPattern,
      rightNode: NodePattern) =>
        LPTRelationshipChain(matchPattern(leftChain).asInstanceOf[LPTRelationshipScan], relationship, rightNode)
    }
  }
}

case class UnknownASTNodeException(node: ASTNode) extends LynxException