package org.opencypher.lynx

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.trees.BottomUp

import scala.collection.mutable.ArrayBuffer

trait QueryPlanner {
  def logicalPlanOptimized(plan: LogicalOperator, context: LogicalPlannerContext): LogicalOperator

  def physicalPlan(parameters: CypherMap, logicalPlan: LogicalOperator, queryLocalCatalog: QueryLocalCatalog): LynxPhysicalOperator
}

class LynxQueryPlanner(implicit propertyGraph: LynxPropertyGraph) extends DefaultQueryPlanner() {
  val optimizationRules = ArrayBuffer[(LogicalPlannerContext) => PartialFunction[LogicalOperator, LogicalOperator]](
    (_) => LogicalOptimizer.discardScansForNonexistentLabels,
    (_) => LogicalOptimizer.replaceCartesianWithValueJoin,
    LogicalOptimizer.replaceScansWithRecognizedPatterns(_)
  )

  def rewrite(rule: (LogicalPlannerContext) => PartialFunction[LogicalOperator, LogicalOperator]): this.type = {
    optimizationRules += rule
    this
  }

  def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    optimizationRules.foldLeft(input) {
      // TODO: Evaluate if multiple rewriters could be fused
      case (tree: LogicalOperator, optimizationRule) => BottomUp[LogicalOperator](optimizationRule(context)).transform(tree)
    }
  }

  //append new rewriter
  rewrite {
    _ => {
      case filter@Filter(expr: Expr, PatternScan(NodePattern(nodeType), _, _, _), solved: SolvedQueryModel) =>
        println(s"!!!!top level filter: $filter")
        filter
    }
  }

  override def logicalPlanOptimized(plan: LogicalOperator, context: LogicalPlannerContext): LogicalOperator = process(plan)(context)
}

class DefaultQueryPlanner(implicit propertyGraph: LynxPropertyGraph) extends QueryPlanner {

  def eval(op: LogicalOperator)(implicit parameters: CypherMap, queryLocalCatalog: QueryLocalCatalog): LynxPhysicalOperator = {
    op match {
      case Start(graph: LogicalGraph, solved: SolvedQueryModel) =>
        graph match {
          case g: LogicalCatalogGraph =>
            StartPipe(g.qualifiedGraphName)
        }

      case Expand(source: Var, rel: Var, target: Var, direction, lhs: LogicalOperator, rhs: LogicalOperator, solved: SolvedQueryModel) =>
        ExpandPipe(eval(lhs), eval(rhs), source: Var, rel: Var, target: Var, direction)

      case Select(fields: List[Var], in, solved) =>
        SelectPipe(eval(in), fields)

      case f@Filter(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel) =>
        f match {
          case Filter(expr: Expr, PatternScan(NodePattern(nodeType), _, _, _), solved: SolvedQueryModel) =>
            TopLevelFilterPipe(solved, parameters)
          case _ => FilterPipe(eval(in), expr, parameters)
        }

      case Project(projectExpr: (Expr, Option[Var]), in: LogicalOperator, solved: SolvedQueryModel) =>
        ProjectPipe(eval(in), projectExpr)

      case PatternScan(pattern: Pattern, mapping: Map[Var, PatternElement], in: LogicalOperator, solved: SolvedQueryModel) =>
        PatternScanPipe(eval(in), pattern: Pattern, mapping)
    }
  }

  override def physicalPlan(parameters: CypherMap, logicalPlan: LogicalOperator, queryLocalCatalog: QueryLocalCatalog) = {
    eval(logicalPlan)(parameters, queryLocalCatalog)
  }

  override def logicalPlanOptimized(plan: LogicalOperator, context: LogicalPlannerContext): LogicalOperator = LogicalOptimizer.process(plan)(context)
}