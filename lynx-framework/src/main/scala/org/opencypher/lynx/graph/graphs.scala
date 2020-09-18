
package org.opencypher.lynx.graph

import org.opencypher.lynx.util.PropertyGraphSchemaOps.PropertyGraphSchemaOps
import org.opencypher.lynx.planning.LynxPhysicalPlanner.PhysicalOperatorOps
import org.opencypher.lynx._
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.impl.util.VarConverters.RichPatternElement

trait Scannable {
  def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false)(implicit ctx: LynxPlannerContext): PhysicalOperator
}

trait LynxPropertyGraph extends PropertyGraph with Scannable {
  override implicit def session: LynxSession

  def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): LynxRecords

  def relationships(name: String, relCypherType: CTRelationship = CTRelationship): LynxRecords

  def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false)(implicit ctx: LynxPlannerContext): PhysicalOperator = {
    val graph = this

    val selectedScans: Seq[PhysicalOperator] = searchPattern.elements.map({
      case PatternElement(name, cypherType@CTNode(knownLabels, _)) if knownLabels.isEmpty =>
        Start.fromRecords(ctx, cypherType.graph.getOrElse(LynxSession.EMPTY_GRAPH_NAME),
          graph.nodes(name, cypherType, exactLabelMatch))

      case PatternElement(name, cypherType@CTNode(knownLabels, _)) =>
        Start.fromRecords(ctx, cypherType.graph.getOrElse(LynxSession.EMPTY_GRAPH_NAME),
          graph.nodes(name, cypherType, exactLabelMatch))

      case PatternElement(name, cypherType@CTRelationship(types, _)) if types.isEmpty =>
        Start.fromRecords(ctx, cypherType.graph.getOrElse(LynxSession.EMPTY_GRAPH_NAME),
          graph.relationships(name, cypherType))

      case PatternElement(name, cypherType@CTRelationship(types, _)) =>
        Start.fromRecords(ctx, cypherType.graph.getOrElse(LynxSession.EMPTY_GRAPH_NAME),
          graph.relationships(name, cypherType))
    }).toSeq

    val schema = graph.schema

    val op: PhysicalOperator = selectedScans.toList match {
      case Nil =>
        val scanHeader = searchPattern
          .elements
          .map { e => schema.headerForElement(e.toVar) }
          .reduce(_ ++ _)

        Start.fromEmptyGraph(LynxRecords.empty(scanHeader))(ctx)

      case singleOp :: Nil =>
        singleOp

      case multipleOps =>
        multipleOps.reduce(TabularUnionAll(_, _))
    }

    op
  }

  override def cypher(
                       query: String,
                       parameters: CypherValue.CypherMap,
                       drivingTable: Option[CypherRecords],
                       queryCatalog: Map[QualifiedGraphName, PropertyGraph]
                     ): CypherResult = session.cypherOnGraph(this, query, parameters, drivingTable, queryCatalog)

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

case class UnionGraph(graphs: Seq[LynxPropertyGraph])(implicit val session: LynxSession) extends LynxPropertyGraph {

  override def patterns: Set[Pattern] =
    graphs
      .map(_.patterns)
      .foldLeft(Set.empty[Pattern])(_ intersect _)

  require(graphs.nonEmpty, "Union requires at least one graph")

  override lazy val schema: PropertyGraphSchema = graphs.map(g => g.schema).foldLeft(PropertyGraphSchema.empty)(_ ++ _)

  override def toString = s"UnionGraph(graphs=[${graphs.mkString(",")}])"

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxRecords =
    graphs.map(_.nodes(name, nodeCypherType, exactLabelMatch)).reduce(_.union(_))

  override def relationships(name: String, relCypherType: CTRelationship): LynxRecords =
    graphs.map(_.relationships(name, relCypherType)).reduce(_.union(_))
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
}