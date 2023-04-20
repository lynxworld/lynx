package org.grapheco.lynx.physical

import org.grapheco.lynx.logical._
import org.grapheco.lynx._
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.ast.{Create, Delete, Merge, MergeAction}
import org.opencypher.v9_0.expressions._

/**
 * @ClassName DefaultPhysicalPlanner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
        PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      case lc@LPTCreate(c: Create) => PPTCreateTranslator(c).translate(lc.in.map(plan(_)))(plannerContext)
      case lm@LPTMerge(m: Merge) => PPTMergeTranslator(m).translate(lm.in.map(plan(_)))(plannerContext)
      case lm@LPTMergeAction(m: Seq[MergeAction]) => PPTMergeAction(m)(plan(lm.in.get), plannerContext)
      case ld@LPTDelete(d: Delete) => PPTDelete(d)(plan(ld.in), plannerContext)
      case ls@LPTSelect(columns: Seq[(String, Option[String])]) => PPTSelect(columns)(plan(ls.in), plannerContext)
      case lp@LPTProject(ri) => PPTProject(ri)(plan(lp.in), plannerContext)
      case la@LPTAggregation(a, g) => PPTAggregation(a, g)(plan(la.in), plannerContext)
      case lc@LPTCreateUnit(items) => PPTCreateUnit(items)(plannerContext)
      case lf@LPTFilter(expr) => PPTFilter(expr)(plan(lf.in), plannerContext)
      case ld@LPTDistinct() => PPTDistinct()(plan(ld.in), plannerContext)
      case ll@LPTLimit(expr) => PPTLimit(expr)(plan(ll.in), plannerContext)
      case lo@LPTOrderBy(sortItem) => PPTOrderBy(sortItem)(plan(lo.in), plannerContext)
      case ll@LPTSkip(expr) => PPTSkip(expr)(plan(ll.in), plannerContext)
      case lj@LPTJoin(isSingleMatch, joinType) => PPTJoin(None, isSingleMatch, joinType)(plan(lj.a), plan(lj.b), plannerContext)
      case patternMatch: LPTPatternMatch => PPTPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
      case li@LPTCreateIndex(labelName: LabelName, properties: List[PropertyKeyName]) => PPTCreateIndex(labelName, properties)(plannerContext)
      case li@LPTDropIndex(labelName: LabelName, properties: List[PropertyKeyName]) => PPTDropIndex(labelName, properties)(plannerContext)
      case sc@LPTSetClause(d) => PPTSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr@LPTRemove(r) => PPTRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
      case lu@LPTUnwind(u) => PPTUnwindTranslator(u.expression, u.variable).translate(lu.in.map(plan(_)))(plannerContext)
    }
  }
}
