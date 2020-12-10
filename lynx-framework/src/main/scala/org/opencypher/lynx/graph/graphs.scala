
package org.opencypher.lynx.graph

import org.opencypher.lynx._
import org.opencypher.lynx.planning.LynxPhysicalPlanner.PhysicalOperatorOps
import org.opencypher.lynx.planning.{PhysicalOperator, Start, TabularUnionAll}
import org.opencypher.lynx.util.PropertyGraphSchemaOps.PropertyGraphSchemaOps
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr.{EndNode, NodeVar, RelationshipVar, StartNode}
import org.opencypher.okapi.ir.impl.util.VarConverters.RichPatternElement

trait LynxPropertyGraph extends PropertyGraph {
  override implicit def session: LynxSession

  val tableOperator = session.tableOperator

  def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): LynxRecords

  def relationships(name: String, relCypherType: CTRelationship = CTRelationship): LynxRecords

  def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false)(implicit ctx: LynxPlannerContext): PhysicalOperator

  override def cypher(
                       query: String,
                       parameters: CypherValue.CypherMap,
                       drivingTable: Option[CypherRecords],
                       queryCatalog: Map[QualifiedGraphName, PropertyGraph]
                     ): CypherResult =
    session.cypherOnGraph(this, query, parameters, drivingTable, queryCatalog)

  override def unionAll(others: PropertyGraph*): LynxPropertyGraph = {
    val otherGraphs: List[LynxPropertyGraph] = others.toList.map {
      case g: LynxPropertyGraph => g
      case _ => throw UnsupportedOperationException("Union all only works on relational graphs")
    }

    val graphAt = (qgn: QualifiedGraphName) => Some(session.catalog.graph(qgn) match {
      case g: LynxPropertyGraph => g.asInstanceOf[LynxPropertyGraph]
    })

    val allGraphs = (this :: otherGraphs).zipWithIndex.map { case (g, i) => LynxPropertyGraph.prefixed(g, i.toByte) }
    LynxPropertyGraph.union(allGraphs: _*)(session)
  }
}

//create PhysicalOperator
object LynxPropertyGraph {
  def empty()(implicit session: LynxSession): LynxPropertyGraph = EmptyGraph()

  def union(graphs: LynxPropertyGraph*)(implicit session: LynxSession): LynxPropertyGraph = UnionGraph(graphs)

  def construct(someSchema: Some[PropertyGraphSchema], seq: Any*): LynxPropertyGraph = ???

  def prefixed(graph: LynxPropertyGraph, prefix: GraphIdPrefix)(implicit session: LynxSession): LynxPropertyGraph = PrefixedGraph(graph, prefix)
}


class ScanGraph[Id](scan: PropertyGraphScan[Id])(implicit val session: LynxSession) extends LynxPropertyGraph {

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords = {
    val nodes = if (nodeCypherType.labels.nonEmpty) {
      scan.allNodes(nodeCypherType.labels, exactLabelMatch)
    }
    else {
      scan.allNodes()
    }
    new LynxRecords(
      RecordHeader(Map(NodeVar(name)(nodeCypherType) -> name)),
      LynxTable(Seq(name -> CTNode), nodes.map(Seq(_)))
    )
  }

  override def relationships(name: String, relCypherType: CTRelationship): LynxRecords = {
    val rels = if (relCypherType.types.nonEmpty) {
      scan.allRelationships(relCypherType.types)
    }
    else {
      scan.allRelationships()
    }

    new LynxRecords(
      RecordHeader(Map(
        RelationshipVar(name)(relCypherType) -> name,
        StartNode(RelationshipVar(name)(relCypherType))(CTNode) -> SourceStartNodeKey.name,
        EndNode(RelationshipVar(name)(relCypherType))(CTNode) -> SourceEndNodeKey.name
      )),
      LynxTable(
        Seq(name -> CTRelationship, SourceStartNodeKey.name -> CTNode, SourceEndNodeKey.name -> CTNode),
        rels.map(rel => Seq(rel, scan.nodeAt(rel.startId), scan.nodeAt(rel.endId))))
    )
  }

  override def schema: PropertyGraphSchema = scan.schema

  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false)(implicit ctx: LynxPlannerContext): PhysicalOperator = {
    val graph = this
    val session = ctx.session
    val selectedScans: Seq[PhysicalOperator] = searchPattern.elements.map({
      case PatternElement(name, cypherType@CTNode(knownLabels, _)) if knownLabels.isEmpty =>
        Start.fromRecords(cypherType.graph,
          graph.nodes(name, cypherType, exactLabelMatch))

      case PatternElement(name, cypherType@CTNode(knownLabels, _)) =>
        Start.fromRecords(cypherType.graph,
          graph.nodes(name, cypherType, exactLabelMatch))

      case PatternElement(name, cypherType@CTRelationship(types, _)) if types.isEmpty =>
        Start.fromRecords(cypherType.graph,
          graph.relationships(name, cypherType))

      case PatternElement(name, cypherType@CTRelationship(types, _)) =>
        Start.fromRecords(cypherType.graph,
          graph.relationships(name, cypherType))
    }).toSeq

    val schema = graph.schema

    val op: PhysicalOperator = selectedScans.toList match {
      case Nil =>
        val scanHeader = searchPattern
          .elements
          .map { e => schema.headerForElement(e.toVar) }
          .reduce(_ ++ _)

        Start.fromRecords(LynxRecords.empty(scanHeader)(ctx.session))(ctx)

      case singleOp :: Nil =>
        singleOp

      case multipleOps =>
        multipleOps.reduce(TabularUnionAll(_, _))
    }

    op
  }
}

case class UnionGraph(graphs: Seq[LynxPropertyGraph])(implicit val session: LynxSession) extends LynxPropertyGraph {

  override def patterns: Set[Pattern] =
    graphs
      .map(_.patterns)
      .foldLeft(Set.empty[Pattern])(_ intersect _)

  require(graphs.nonEmpty, "Union requires at least one graph")

  override lazy val schema: PropertyGraphSchema = graphs.map(g => g.schema).foldLeft(PropertyGraphSchema.empty)(_ ++ _)

  override def toString = s"UnionGraph(graphs=[${graphs.mkString(",")}])"

  def union(a: LynxRecords, b: LynxRecords) = LynxRecords(
    RecordHeader(a.header.exprToColumn ++ b.header.exprToColumn),
    session.tableOperator.unionAll(a.table, b.table),
    a.logicalColumns
  )

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords =
    graphs.map(_.nodes(name, nodeCypherType, exactLabelMatch)).reduce(union(_, _))

  override def relationships(name: String, relCypherType: CTRelationship): LynxRecords =
    graphs.map(_.relationships(name, relCypherType)).reduce(union(_, _))

  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean)(implicit ctx: LynxPlannerContext): PhysicalOperator = ???
}

case class PrefixedGraph(graph: LynxPropertyGraph, prefix: GraphIdPrefix)
                        (implicit val session: LynxSession) extends LynxPropertyGraph {

  override lazy val schema: PropertyGraphSchema = graph.schema

  override def toString = s"PrefixedGraph(graph=$graph)"

  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false)(implicit ctx: LynxPlannerContext): PhysicalOperator = {
    searchPattern.elements.foldLeft(graph.scanOperator(searchPattern, exactLabelMatch)) {
      case (acc, patternElement) => acc.prefixVariableId(patternElement.toVar, prefix)
    }
  }

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords = graph.nodes(name, nodeCypherType, exactLabelMatch)

  override def relationships(name: String, relCypherType: CTRelationship): LynxRecords = graph.relationships(name, relCypherType)
}

case class EmptyGraph()(implicit val session: LynxSession) extends LynxPropertyGraph {
  override val schema: PropertyGraphSchema = PropertyGraphSchema.empty

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords = LynxRecords.empty()

  override def relationships(name: String, relCypherType: CTRelationship): LynxRecords = LynxRecords.empty()

  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean)(implicit ctx: LynxPlannerContext): PhysicalOperator = {
    val context: LynxPlannerContext = session.createPlannerContext()

    val scanHeader = searchPattern.elements
      .map { e => RecordHeader.from(e.toVar) }
      .reduce(_ ++ _)

    val records = LynxRecords.empty(scanHeader)
    Start.fromRecords(records)(context)
  }
}