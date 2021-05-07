package org.grapheco.lynx

import org.opencypher.v9_0.ast.{AliasedReturnItem, Clause, Create, CreateIndex, CreateUniquePropertyConstraint, Limit, Match, OrderBy, PeriodicCommitHint, ProcedureResult, ProcedureResultItem, Query, QueryPart, Return, ReturnItem, ReturnItems, ReturnItemsDef, SingleQuery, Skip, SortItem, Statement, UnresolvedCall, Where, With}
import org.opencypher.v9_0.expressions.{EveryPath, Expression, FunctionInvocation, FunctionName, LabelName, LogicalVariable, Namespace, NodePattern, Pattern, PatternElement, PatternPart, ProcedureName, Property, PropertyKeyName, RelationshipChain, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.{ASTNode, InputPosition}

//logical plan tree node (operator)
trait LPTNode extends TreeNode {
  override type SerialType = LPTNode
  override val children: Seq[LPTNode] = Seq.empty
}

trait LogicalPlanner {
  def plan(statement: Statement, plannerContext: LogicalPlannerContext): LPTNode
}

//translates an ASTNode into a LPTNode, `in` as input operator
trait LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode
}

//pipelines a set of LPTNodes
case class PipedTranslators(items: Seq[LPTNodeTranslator]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    items.foldLeft[Option[LPTNode]](in) {
      (in, item) => Some(item.translate(in)(plannerContext))
    }.get
  }
}

class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LPTNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LPTQueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(Variable(v1),LabelName(l),List(Property(Variable(v2),PropertyKeyName(p)))) =>
        throw UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) =>
        LPTCreateIndex(labelName, properties)

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LPTNode = {
    translate(statement)(plannerContext)
  }
}

/////////////////ProcedureCall/////////////
case class LPTProcedureCallTranslator(c: UnresolvedCall) extends LPTNodeTranslator {
  override def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    val UnresolvedCall(ns@Namespace(parts: List[String]), pn@ProcedureName(name: String), declaredArguments: Option[Seq[Expression]], declaredResult: Option[ProcedureResult]) = c
    val call = LPTProcedureCall(ns, pn, declaredArguments)

    declaredResult match {
      case Some(ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where])) =>
        PipedTranslators(Seq(LPTSelectTranslator(items.map(
          item => {
            val ProcedureResultItem(output, Variable(varname)) = item
            varname -> output.map(_.name)
          }
        )), LPTWhereTranslator(where))).translate(Some(call))

      case None => call
    }
  }
}

case class LPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) extends LPTNode

//////////////////Create////////////////
case class LPTCreateTranslator(c: Create) extends LPTNodeTranslator {
  override def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode =
    LPTCreate(c)(in)
}

case class LPTCreate(c: Create)(val in: Option[LPTNode]) extends LPTNode {
}

///////////////////////////////////////
case class LPTQueryPartTranslator(part: QueryPart) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        PipedTranslators(
          clauses.map(
            _ match {
              case c: UnresolvedCall => LPTProcedureCallTranslator(c)
              case r: Return => LPTReturnTranslator(r)
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
case class LPTReturnTranslator(r: Return) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    val Return(distinct, ri, orderBy, skip, limit, excludedNames) = r

    PipedTranslators(
      Seq(
        LPTProjectTranslator(ri),
        LPTSkipTranslator(skip),
        LPTLimitTranslator(limit),
        LPTOrderByTranslator(orderBy),
        LPTSelectTranslator(ri),
        LPTDistinctTranslator(distinct)
      )).translate(in)
  }
}


case class LPTOrderByTranslator(orderBy: Option[OrderBy]) extends LPTNodeTranslator{
  override def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    orderBy match {
      case None => in.get
      case Some(value) => LPTOrderBy(value.sortItems)(in.get)
    }
  }

}

case class LPTOrderBy(sortItem: Seq[SortItem])(val in: LPTNode) extends LPTNode{
  override val children: Seq[LPTNode] = Seq(in)
}
object LPTSelectTranslator {
  def apply(ri: ReturnItemsDef): LPTSelectTranslator = LPTSelectTranslator(ri.items.map(item => item.name -> item.alias.map(_.name)))
}

case class LPTSelectTranslator(columns: Seq[(String, Option[String])]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    LPTSelect(columns)(in.get)
  }
}

case class LPTSelect(columns: Seq[(String, Option[String])])(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

case class LPTProject(ri: ReturnItemsDef)(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

case class LPTLimit(expression: Expression)(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

case class LPTSkip(expression: Expression)(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

case class LPTFilter(expr: Expression)(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

case class LPTWhereTranslator(where: Option[Where]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    where match {
      case None => in.get
      case Some(Where(expr)) => LPTFilter(expr)(in.get)
    }
  }
}

case class LPTWithTranslator(w: With) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
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
            LPTOrderByTranslator(orderBy),
            LPTSelectTranslator(ri),
            LPTDistinctTranslator(distinct)
          )).translate(in)
    }
  }
}

case class LPTCreateUnit(items: Seq[ReturnItem]) extends LPTNode {
}

case class LPTProjectTranslator(ri: ReturnItemsDef) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    val preojectIn = in match {
      case Some(sin) => sin
      case None => LPTCreateUnit(ri.items)
    }
    LPTProject(ri)(preojectIn)
  }
}

case class LPTLimitTranslator(limit: Option[Limit]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    limit match {
      case None => in.get
      case Some(Limit(expr)) => LPTLimit(expr)(in.get)
    }
  }
}

case class LPTDistinctTranslator(distinct: Boolean) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    distinct match {
      case false => in.get
      case true => LPTDistinct()(in.get)
    }
  }
}

case class LPTCreateIndex(labelName: LabelName, properties: List[PropertyKeyName]) extends LPTNode

trait LogicalQueryPart extends LPTNode
case class LPTSkipTranslator(skip: Option[Skip]) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    skip match {
      case None => in.get
      case Some(Skip(expr)) => LPTSkip(expr)(in.get)
    }
  }
}

case class LPTDistinct()(val in: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(in)
}

///////////////////////////////////////
case class LPTJoin()(val a: LPTNode, val b: LPTNode) extends LPTNode {
  override val children: Seq[LPTNode] = Seq(a, b)
}

/////////////////match/////////////////
case class LPTPatternMatch(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)])
  extends LPTNode {
}

case class LPTMatchTranslator(m: Match) extends LPTNodeTranslator {
  def translate(in: Option[LPTNode])(implicit plannerContext: LogicalPlannerContext): LPTNode = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val parts = patternParts.map(matchPatternPart(_)(plannerContext))
    val matched = (parts.drop(1)).foldLeft(parts.head)((a, b) => LPTJoin()(a, b))
    val filtered = LPTWhereTranslator(where).translate(Some(matched))

    in match {
      case None => filtered
      case Some(left) => LPTJoin()(left, filtered)
    }
  }

  private def matchPatternPart(patternPart: PatternPart)(implicit lpc: LogicalPlannerContext): LPTNode = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def matchPattern(element: PatternElement)(implicit lpc: LogicalPlannerContext): LPTPatternMatch = {
    element match {
      //match (m:label1)
      case np: NodePattern =>
        LPTPatternMatch(np, Seq.empty)

      //match ()-[]->()
      case rc@RelationshipChain(
      leftNode: NodePattern,
      relationship: RelationshipPattern,
      rightNode: NodePattern) =>
        LPTPatternMatch(leftNode, Seq(relationship -> rightNode))

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc@RelationshipChain(
      leftChain: RelationshipChain,
      relationship: RelationshipPattern,
      rightNode: NodePattern) =>
        val mp = matchPattern(leftChain)
        LPTPatternMatch(mp.headNode, mp.chain :+ (relationship -> rightNode))
    }
  }
}
//from neo4j Aggregation
//case class LPTAggregation(left: LPTNode,
//                       groupingExpressions: Map[String, Expression],
//                       aggregationExpression: Map[String, Expression])
//                      (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with EagerLogicalPlan {
//
//  def ap(newSolved: PlannerQuery with CardinalityEstimation) = copy()(newSolved)
//
//  val lhs = Some(left)
//
//  def rhs = None
//
//  val groupingKeys = groupingExpressions.keySet.map(IdName(_))
//
//  val availableSymbols = groupingKeys ++ aggregationExpression.keySet.map(IdName(_))
//}

case class UnknownASTNodeException(node: ASTNode) extends LynxException