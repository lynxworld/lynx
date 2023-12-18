package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{LogicalPlan, LogicalUnion}
import org.opencypher.v9_0.ast._

case class QueryPartTranslator(part: QueryPart) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        PipedTranslators(
          clauses.map {
            case c: UnresolvedCall => CallTranslator(c)
            case r: Return => ReturnTranslator(r)
            case w: With => WithTranslator(w)
            case u: Unwind => UnwindTranslator(u)
            case m: Match => MatchTranslator(m)
            case c: Create => CreateTranslator(c)
            case m: Merge => MergeTranslator(m)
            case d: Delete => DeleteTranslator(d)
            case s: SetClause => SetTranslator(s)
            case r: Remove => RemoveTranslator(r)
            case f: Foreach => ForeachTranslator(f)
          }
        ).translate(in)
      case UnionAll(part, query) => LogicalUnion(distinct = false)(QueryPartTranslator(part).translate(None), QueryPartTranslator(query).translate(None))
      case UnionDistinct(part, query) => LogicalUnion(distinct = true)(QueryPartTranslator(part).translate(None), QueryPartTranslator(query).translate(None))
    }
  }
}
