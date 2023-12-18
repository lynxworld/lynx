package org.grapheco.lynx.physical

import org.grapheco.lynx.logical._
import org.grapheco.lynx._
import org.grapheco.lynx.logical.plans.{LogicalAggregation, LogicalApply, LogicalCreate, LogicalCreateIndex, LogicalCreateUnit, LogicalDelete, LogicalDistinct, LogicalDropIndex, LogicalFilter, LogicalJoin, LogicalLimit, LogicalMerge, LogicalOrderBy, LogicalPatternMatch, LogicalPlan, LogicalProcedureCall, LogicalProject, LogicalRemove, LogicalSelect, LogicalSetClause, LogicalShortestPaths, LogicalSkip, LogicalUnion, LogicalUnwind, LogicalWith}
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.ast.{Create, Delete, Merge, MergeAction, ReturnItems}
import org.opencypher.v9_0.expressions._

/**
 * @ClassName DefaultPhysicalPlanner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LogicalPlan)(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LogicalProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
        PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      case lc@LogicalCreate(pattern) => PPTCreateTranslator(pattern).translate(lc.in.map(plan(_)))(plannerContext)
      case lm@LogicalMerge(pattern, actions) => PPTMergeTranslator(pattern, actions).translate(lm.in.map(plan(_)))(plannerContext)
//      case lm@LPTMergeAction(m: Seq[MergeAction]) => PPTMergeAction(m)(plan(lm.in.get), plannerContext)
      case ld@LogicalDelete(expressions,forced) => PPTDelete(expressions, forced)(plan(ld.in), plannerContext)
      case ls@LogicalSelect(columns: Seq[(String, Option[String])]) => PPTSelect(columns)(plan(ls.in), plannerContext)
      case lp@LogicalProject(ri) => PPTProject(ri)(plan(lp.in), plannerContext)
      case la@LogicalAggregation(a, g) => PPTAggregation(a, g)(plan(la.in), plannerContext)
      case lc@LogicalCreateUnit(items) => PPTCreateUnit(items)(plannerContext)
      case lf@LogicalFilter(expr) => PPTFilter(expr)(plan(lf.in), plannerContext)
      case lw@LogicalWith(ri) => PPTWith()(plan(lw.in), plannerContext)
      case ld@LogicalDistinct() => PPTDistinct()(plan(ld.in), plannerContext)
      case ll@LogicalLimit(expr) => PPTLimit(expr)(plan(ll.in), plannerContext)
      case lo@LogicalOrderBy(sortItem) => PPTOrderBy(sortItem)(plan(lo.in), plannerContext)
      case ll@LogicalSkip(expr) => PPTSkip(expr)(plan(ll.in), plannerContext)
      case lj@LogicalJoin(isSingleMatch, joinType) => PPTJoin(None, isSingleMatch, joinType)(plan(lj.a), plan(lj.b), plannerContext)
      case ap@LogicalApply(ri) => PPTApply()(plan(ap.left), plan(ap.right)(plannerContext.withArgumentsContext(ri.map(_.name))), plannerContext.withArgumentsContext(ri.map(_.name)))
      case patternMatch: LogicalPatternMatch => PPTPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
      case lPTShortestPaths : LogicalShortestPaths => LPTShortestPathTranslator(lPTShortestPaths)(plannerContext).translate(None)
      case li@LogicalCreateIndex(labelName: String, properties: List[String]) => PPTCreateIndex(labelName, properties)(plannerContext)
      case li@LogicalDropIndex(labelName: String, properties: List[String]) => PPTDropIndex(labelName, properties)(plannerContext)
      case sc@LogicalSetClause(d) => PPTSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr@LogicalRemove(r) => PPTRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
      case lu@LogicalUnwind(u) => PPTUnwindTranslator(u.expression, u.variable).translate(lu.in.map(plan(_)))(plannerContext)
      case un@LogicalUnion(distinct) => PPTUnion(distinct)(plan(un.a), plan(un.b), plannerContext)
      case _ => throw new Exception("physical plan not support:" +logicalPlan)
    }
  }
}
