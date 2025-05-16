package org.grapheco.lynx.physical.planner

import org.grapheco.lynx.logical.plans._
import org.grapheco.lynx.physical._
import org.grapheco.lynx.physical.planner.translators.{GraphPatternMatchTranslator, LPTShortestPathTranslator, PPTCreateTranslator, PPTMergeTranslator, PPTPatternMatchTranslator, PPTRemoveTranslator, PPTSetClauseTranslator}
import org.grapheco.lynx.physical.plans.{Aggregation, Apply, CreateIndex, CreateUnit, Cross, Delete, Distinct, DropIndex, Filter, Join, Limit, OrderBy, PhysicalPlan, PhysicalPlanBuffer, ProcedureCall, Project, Select, Skip, Union, Unwind, With}
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.expressions._

/**
 * @ClassName DefaultPhysicalPlanner
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LogicalPlan)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {

      // With 2 Child
      case un@LogicalUnion(distinct) => Union(distinct).withChildren(un.left.map(plan), un.right.map(plan))
      case lj@LogicalJoin(isSingleMatch, joinType) => Join(None, isSingleMatch, joinType).withChildren(lj.left.map(plan), lj.right.map(plan))
      case lc@LogicalCross() => Cross().withChildren(lc.left.map(plan), lc.right.map(plan))
      case ap@LogicalAndThen(joinType) => {
        val first = plan(ap.first)
        val contextWithArg: PhysicalPlannerContext = plannerContext.withArgumentsContext(first.schema.map(_._1))
        val andThen = plan(ap._then)(contextWithArg)
        Apply(joinType)(contextWithArg).withChildren(Some(first), Some(andThen))
      }
      case aj@LogicalAndThenJoin(isSingleMatch, joinType) => {
        val first = plan(aj.first)
        val contextWithArg: PhysicalPlannerContext = plannerContext.withArgumentsContext(first.schema.map(_._1))
        val andThen = plan(aj._then)(contextWithArg)
        Join(None, isSingleMatch, joinType)(contextWithArg).withChildren(Some(first), Some(andThen))
      }
      // Used Translator
      case pm:LogicalPatternMatch  => PPTPatternMatchTranslator(pm)(plannerContext).translate(None)
      case gp:GraphPatternMatch    => GraphPatternMatchTranslator(gp)(plannerContext).translate(None)
      case sp:LogicalShortestPaths => LPTShortestPathTranslator(sp)(plannerContext).translate(None)
      case lc@LogicalCreate(pattern) => PPTCreateTranslator(pattern).translate(lc.in.map(plan(_)))(plannerContext)
      case lm@LogicalMerge(pattern, actions) => PPTMergeTranslator(pattern, actions).translate(lm.in.map(plan(_)))(plannerContext)
      case sc@LogicalSetClause(d) => PPTSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr@LogicalRemove(r) => PPTRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
      case single: SingleLogicalPlan => (single match {
        case LogicalAggregation(a, g) => Aggregation(a, g)
        case LogicalDelete(expressions, forced) => Delete(expressions, forced)
        case LogicalDistinct() => Distinct()
        case LogicalFilter(expr) => Filter(expr)
        case LogicalLimit(expr) => Limit(expr)
        case LogicalOrderBy(sortItem) => OrderBy(sortItem)
        case LogicalProject(ri) => Project(ri)
        case LogicalSelect(columns: Seq[(String, Option[String])]) => Select(columns)
        case LogicalSkip(expr) => Skip(expr)
        case LogicalUnwind(u) => Unwind(u.expression, u.variable)
        case LogicalWith(ri) => With(ri)
      }).withChildren(single.left.map(plan))
      case leaf: LeafLogicalPlan => leaf match {
        case LogicalCreateIndex(labelName: String, properties: List[String]) => CreateIndex(labelName, properties)
        case LogicalCreateUnit(items) => CreateUnit(items)
        case LogicalDropIndex(labelName: String, properties: List[String]) => DropIndex(labelName, properties)
        case LogicalProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
          ProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      }
      case single: SingleLogicalPlan => (single match {
        case LogicalAggregation(a, g) => Aggregation(a, g)
        case LogicalDelete(expressions, forced) => Delete(expressions, forced)
        case LogicalDistinct() => Distinct()
        case LogicalFilter(expr) => Filter(expr)
        case LogicalLimit(expr) => Limit(expr)
        case LogicalOrderBy(sortItem) => OrderBy(sortItem)
        case LogicalProject(ri) => Project(ri)
        case LogicalSelect(columns: Seq[(String, Option[String])]) => Select(columns)
        case LogicalSkip(expr) => Skip(expr)
        case LogicalUnwind(u) => Unwind(u.expression, u.variable)
        case LogicalWith(ri) => With(ri)
      }).withChildren(single.left.map(plan))
      case leaf: LeafLogicalPlan => leaf match {
        case LogicalCreateIndex(labelName: String, properties: List[String]) => CreateIndex(labelName, properties)
        case LogicalCreateUnit(items) => CreateUnit(items)
        case LogicalDropIndex(labelName: String, properties: List[String]) => DropIndex(labelName, properties)
        case LogicalProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
          ProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      }
      case _ => throw new Exception("physical plan not support:" +logicalPlan)
    }
  }
}
