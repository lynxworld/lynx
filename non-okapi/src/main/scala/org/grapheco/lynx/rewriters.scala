package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.opencypher.v9_0.frontend.phases.{BaseContext, BaseState, Condition, Phase}
import org.opencypher.v9_0.rewriting.rewriters.{expandCallWhere, mergeInPredicates}
import org.opencypher.v9_0.util._

import scala.collection.mutable

//come from okapi
case object LynxPreparatoryRewriting extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val rewrittenStatement = from.statement().endoRewrite(inSequence(
      normalizeReturnClauses(context.exceptionCreator),
      expandCallWhere,
      mergeInPredicates))

    from.withStatement(rewrittenStatement)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions: Set[Condition] = Set.empty
}

case class normalizeReturnClauses(mkException: (String, InputPosition) => CypherException) extends Rewriter {
  def apply(that: AnyRef): AnyRef = instance.apply(that)
  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause@Return(_, ri@ReturnItems(_, items), None, _, _, _) =>
      val aliasedItems = items.map({
        case i: AliasedReturnItem =>
          i
        case i =>
          val newPosition = i.expression.position.bumped()
          AliasedReturnItem(i.expression, Variable(i.name)(newPosition))(i.position)
      })
      Seq(
        clause.copy(returnItems = ri.copy(items = aliasedItems)(ri.position))(clause.position)
      )
    case clause@Return(distinct, ri: ReturnItems, orderBy, skip, limit, _) =>
      clause.verifyOrderByAggregationUse((s, i) => throw mkException(s, i))
      var rewrites = mutable.Map[Expression, Variable]()
      val (aliasProjection, finalProjection) = ri.items.map {
        i =>
          val returnColumn = i.alias match {
            case Some(alias) => alias
            case None => Variable(i.name)(i.expression.position.bumped())
          }
          val newVariable = Variable(FreshIdNameGenerator.name(i.expression.position))(i.expression.position)
          // Always update for the return column, so that it has precedence over the expressions (if there are variables with the same name),
          // e.g. match (n),(m) return n as m, m as m2
          rewrites += (returnColumn -> newVariable)
          // Only update if rewrites does not yet have a mapping for i.expression
          rewrites.getOrElseUpdate(i.expression, newVariable)
          (AliasedReturnItem(i.expression, newVariable)(i.position), AliasedReturnItem(newVariable.copyId, returnColumn)(i.position))
      }.unzip
      val newOrderBy = orderBy.endoRewrite(topDown(Rewriter.lift {
        case exp: Expression if rewrites.contains(exp) => rewrites(exp).copyId
      }))
      val introducedVariables = if (ri.includeExisting) aliasProjection.map(_.variable.name).toSet else Set.empty[String]
      Seq(
        With(distinct = distinct, returnItems = ri.copy(items = aliasProjection)(ri.position),
          orderBy = newOrderBy, skip = skip, limit = limit, where = None)(clause.position),
        Return(distinct = false, returnItems = ri.copy(items = finalProjection)(ri.position),
          orderBy = None, skip = None, limit = None, excludedNames = introducedVariables)(clause.position)
      )
    case clause =>
      Seq(clause)
  }
  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  })
}