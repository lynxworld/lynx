package org.opencypher.lynx

import org.opencypher.okapi.api.graph.{NodePattern, Pattern, PatternElement, QualifiedGraphName}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Expr, NodeVar, RelationshipVar, Var}
import org.opencypher.okapi.logical.impl.{Direction, LogicalCatalogGraph, LogicalGraph, SolvedQueryModel}
import org.opencypher.okapi.trees.AbstractTreeNode

case class LynxPhysicalPlanContext(parameters: CypherMap) {

}

abstract class LynxPhysicalOperator extends AbstractTreeNode[LynxPhysicalOperator] {
  def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords
}

case class StartPipe(qualifiedGraphName: QualifiedGraphName) extends LynxPhysicalOperator {

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = {
    //val records = ctx.inputRecords
    new LynxCypherRecords(Map.empty, Stream.empty)
  }
}

case class ExpandPipe(lhs: LynxPhysicalOperator, rhs: LynxPhysicalOperator, source: Var, rel: Var, target: Var, direction: Direction)(implicit propertyGraph: LynxPropertyGraph) extends LynxPhysicalOperator {
  override val children: Array[LynxPhysicalOperator] = Array(lhs, rhs)

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = (source, target, rel, direction) match {
    case (NodeVar(name1), NodeVar(name2), rv: RelationshipVar, _) =>
      val rels = propertyGraph.scanRelationships(rv.cypherType.asInstanceOf[CTRelationship].types.headOption)
      new LynxCypherRecords(Map(name1 -> CTNode, name2 -> CTNode, rv.name -> CTRelationship),
        rels.map(rel => CypherMap(
          name1 -> propertyGraph.node(rel.startId),
          name2 -> propertyGraph.node(rel.endId),
          rv.name -> rel
        )))
  }
}

case class SelectPipe(in: LynxPhysicalOperator, fields: List[Var]) extends LynxPhysicalOperator {
  override val children: Array[LynxPhysicalOperator] = Array(in)

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = in.execute(ctx).select(fields.map(_.name).toArray)
}

case class ProjectPipe(in: LynxPhysicalOperator, projectExpr: (Expr, Option[Var])) extends LynxPhysicalOperator {
  override val children: Array[LynxPhysicalOperator] = Array(in)

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = {
    val records = in.execute(ctx)
    val (ex1, var1) = projectExpr
    records.project(ex1, ctx.parameters)
  }
}

case class TopLevelFilterPipe(model: SolvedQueryModel, parameters: CypherMap)(implicit propertyGraph: LynxPropertyGraph) extends LynxPhysicalOperator {
  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = {
    val SolvedQueryModel(fields, predicates) = model
    propertyGraph.filterNodes(fields, predicates, Set(), parameters)
  }
}

case class FilterPipe(in: LynxPhysicalOperator, expr: Expr, parameters: CypherMap) extends LynxPhysicalOperator {
  override val children: Array[LynxPhysicalOperator] = Array(in)

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = {
    in.execute(ctx).filter(expr, parameters)
  }
}

case class PatternScanPipe(in: LynxPhysicalOperator, pattern: Pattern, mapping: Map[Var, PatternElement])(implicit propertyGraph: LynxPropertyGraph) extends LynxPhysicalOperator {
  override val children: Array[LynxPhysicalOperator] = Array(in)

  override def execute(ctx: LynxPhysicalPlanContext): LynxCypherRecords = pattern match {
    case np: NodePattern => propertyGraph.nodes(mapping.head._1.name, CTNode, true)
  }
}