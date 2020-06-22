package org.opencypher.lynx

import org.opencypher.okapi.api.graph.{NodePattern, Pattern, PatternElement}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Expr, NodeVar, RelationshipVar, Var}
import org.opencypher.okapi.logical.impl.{Direction, SolvedQueryModel}

trait LynxQueryPipe {
  def execute(): LynxCypherRecords
}

case class ExpandPipe(source: Var, rel: Var, target: Var, direction: Direction)(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = (source, target, rel, direction) match {
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

case class SelectPipe(in: LynxQueryPipe, fields: List[Var])(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = in.execute().select(fields.map(_.name).toArray)
}

case class ProjectPipe(in: LynxQueryPipe, projectExpr: (Expr, Option[Var]))(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = {
    val nodes = in.execute()
    val (ex1, var1) = projectExpr
    nodes.project(ex1)
  }
}

case class TopLevelFilterPipe(model: SolvedQueryModel, parameters: CypherMap)(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = {
    val SolvedQueryModel(fields, predicates) = model
    propertyGraph.filterNodes(fields, predicates, Set(), parameters)
  }
}

case class FilterPipe(in: LynxQueryPipe, expr: Expr, parameters: CypherMap)(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = {
    in.execute().filter(expr, parameters)
  }
}

case class PatternScanPipe(graph: LynxPropertyGraph, pattern: Pattern, mapping: Map[Var, PatternElement])(implicit propertyGraph: LynxPropertyGraph) extends LynxQueryPipe {
  override def execute(): LynxCypherRecords = pattern match {
    case np: NodePattern => graph.nodes(mapping.head._1.name, CTNode, true)
  }
}