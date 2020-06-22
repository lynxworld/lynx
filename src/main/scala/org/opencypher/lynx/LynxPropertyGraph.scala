package org.opencypher.lynx

import java.util.concurrent.atomic.AtomicLong

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.trees.BottomUp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/4/28.
  */
class InMemoryPropertyGraph extends LynxPropertyGraph with Logging {
  val nodes = mutable.Map[Long, LynxNode]();
  val rels = mutable.Map[Long, LynxRelationship]();
  val nodeId = new AtomicLong(1)
  val relId = new AtomicLong(1)

  def filterNodesApproximately(expr: Expr, labels: Set[String], parameters: CypherMap): Stream[LynxNode] = {
    logger.debug(s"filterNodesApproximately: $expr")
    scanNodes(labels, true)
  }

  def addNode(labels: Set[String], prop: (String, Any)*) = {
    val node = LynxNode(nodeId.getAndIncrement(), labels, CypherMap(prop: _*))
    nodes += node.id -> node
    node
  }

  def addRelationship(srcNodeId: Long, targetNodeId: Long, relName: String, prop: (String, Any)*) = {
    val rel = LynxRelationship(relId.getAndIncrement(), srcNodeId, targetNodeId, relName, CypherMap(prop: _*))
    rels += rel.id -> rel
    rel
  }

  def scanNodes(labels: Set[String], exactLabelMatch: Boolean): Stream[LynxNode] =
    labels match {
      case _ if labels.isEmpty => nodes.values.toStream
      case _ if (exactLabelMatch) =>
        nodes.values.filter(x => x.labels.eq(labels)).toStream
      case _ =>
        nodes.values.filter(x => x.labels.find(y => labels.contains(y)).isDefined).toStream
    }

  def scanRelationships(relCypherTypeOption: Option[String]): Stream[LynxRelationship] =
    relCypherTypeOption.map(relCypherType => rels.values.filter(_.relType.equals(relCypherType))).getOrElse(rels.values).toStream

  override def node(id: Long): LynxNode = nodes(id)

  override def relationship(id: Long): LynxRelationship = rels(id)
}

abstract class LynxPropertyGraph extends PropertyGraph {
  def node(id: Long): LynxNode

  def relationship(id: Long): LynxRelationship

  def scanNodes(labels: Set[String], exactLabelMatch: Boolean): Stream[LynxNode]

  def scanRelationships(relCypherType: Option[String]): Stream[LynxRelationship]

  def filterNodesApproximately(expr: Expr, labels: Set[String], parameters: CypherMap): Stream[LynxNode]

  def filterNodes(fields: Set[IRField], predicates: Set[Expr], labels: Set[String], parameters: CypherMap): LynxCypherRecords = {
    val nodes = filterNodesApproximately(predicates.head, labels, parameters)
    //2nd filtering
    val rec = LynxCypherRecords.nodes(fields.head.name, nodes)
    rec.filter(predicates.head, parameters)
  }

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxCypherRecords =
    LynxCypherRecords.nodes(name, scanNodes(nodeCypherType.labels, exactLabelMatch))

  override def relationships(name: String, relCypherType: CTRelationship): LynxCypherRecords =
    LynxCypherRecords.rels(name, scanRelationships(Some(relCypherType.name)))

  var _session: CypherSession = null

  def setSession(session: CypherSession) = _session = session

  override def session: CypherSession = _session

  override def unionAll(others: PropertyGraph*): PropertyGraph = ???

  override def schema: PropertyGraphSchema = PropertyGraphSchema.empty.withNodePropertyKeys("person")("name" -> CTString, "age" -> CTInteger)
}

class LynxExecutor(implicit propertyGraph: LynxPropertyGraph) extends DefaultExecutor() {
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

  override def optimize(plan: LogicalOperator, context: LogicalPlannerContext): LogicalOperator = process(plan)(context)
}

class DefaultExecutor(implicit propertyGraph: LynxPropertyGraph) extends QueryPlanExecutor {

  def eval(parameters: CypherMap, op: LogicalOperator, queryLocalCatalog: QueryLocalCatalog): LynxQueryPipe = {
    op match {
      case Expand(source: Var, rel: Var, target: Var, direction, lhs: LogicalOperator, rhs: LogicalOperator, solved: SolvedQueryModel) =>
        ExpandPipe(source: Var, rel: Var, target: Var, direction)

      case Select(fields: List[Var], in, solved) =>
        SelectPipe(eval(parameters, in, queryLocalCatalog), fields)

      case f@Filter(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel) =>
        f match {
          case Filter(expr: Expr, PatternScan(NodePattern(nodeType), _, _, _), solved: SolvedQueryModel) =>
            TopLevelFilterPipe(solved, parameters)
          case _ => FilterPipe(eval(parameters, in, queryLocalCatalog), expr, parameters)
        }

      case Project(projectExpr: (Expr, Option[Var]), in: LogicalOperator, solved: SolvedQueryModel) =>
        ProjectPipe(eval(parameters, in, queryLocalCatalog), projectExpr)

      case PatternScan(pattern: Pattern, mapping: Map[Var, PatternElement], in: LogicalOperator, solved: SolvedQueryModel) =>
        PatternScanPipe(propertyGraph, pattern: Pattern, mapping)
    }
  }

  override def execute(parameters: CypherMap, logicalPlan: LogicalOperator, queryLocalCatalog: QueryLocalCatalog): CypherResult = {
    val res = eval(parameters, logicalPlan, queryLocalCatalog).execute()
    new CypherResult {
      type Records = LynxCypherRecords

      type Graph = PropertyGraph

      override def getGraph: Option[Graph] = Some(propertyGraph)

      override def getRecords: Option[Records] = Some(res)

      override def plans: CypherQueryPlans = new CypherQueryPlans() {
        override def logical: String = "logical"

        override def relational: String = "relational"
      }

      override def show(implicit options: PrintOptions): Unit = res.show(options)
    }
  }

  override def optimize(plan: LogicalOperator, context: LogicalPlannerContext): LogicalOperator = LogicalOptimizer.process(plan)(context)
}

