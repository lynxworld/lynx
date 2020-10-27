package org.opencypher.lynx

import cats.data.NonEmptyList
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.planning.{Ascending, Descending, JoinType}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTInteger, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.{LogicalCatalogGraph, LogicalPatternGraph}
import org.opencypher.okapi.trees.AbstractTreeNode

abstract class PhysicalOperator extends AbstractTreeNode[PhysicalOperator] {

  def recordHeader: RecordHeader = children.head.recordHeader

  lazy val _table: LynxDataFrame = children.head.table

  implicit def context: LynxPlannerContext = children.head.context

  implicit def session: LynxSession = context.session

  def graph: LynxPropertyGraph = children.head.graph

  def graphName: QualifiedGraphName = children.head.graphName

  def maybeReturnItems: Option[Seq[Var]] = children.head.maybeReturnItems

  protected def resolve(qualifiedGraphName: QualifiedGraphName)
                       (implicit context: LynxPlannerContext): LynxPropertyGraph =
    context.resolveGraph(qualifiedGraphName)

  lazy val table: LynxDataFrame = {
    val t = _table

    if (t.physicalColumns.toSet != recordHeader.columns) {
      // Ensure no duplicate columns in initialData
      val initialDataColumns = t.physicalColumns

      val duplicateColumns = initialDataColumns.groupBy(identity).collect {
        case (key, values) if values.size > 1 => key
      }

      if (duplicateColumns.nonEmpty)
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: a table with distinct columns",
          s"a table with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}")

      // Verify that all header column names exist in the data
      val headerColumnNames = recordHeader.columns
      val dataColumnNames = t.physicalColumns.toSet
      val missingTableColumns = headerColumnNames -- dataColumnNames
      if (missingTableColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: table with columns ${recordHeader.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"""|table with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}
              |column(s) ${missingTableColumns.mkString(", ")} are missing in the table
           """.stripMargin
        )
      }

      val missingHeaderColumns = dataColumnNames -- headerColumnNames
      if (missingHeaderColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"data with columns ${recordHeader.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
        )
      }

      // Verify column types
      recordHeader.expressions.foreach { expr =>
        val tableType = t.columnType(recordHeader.column(expr))
        val headerType = expr.cypherType
        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect elements (alternative: change reverse-mapping function somehow)

        headerType match {
          case n if n.subTypeOf(CTNode.nullable) && tableType == CTInteger =>
          case r if r.subTypeOf(CTRelationship.nullable) && tableType == CTInteger =>
          case _ if tableType == headerType =>
          case _ => throw IllegalArgumentException(
            s"${getClass.getSimpleName}: data matching header type $headerType for expression $expr", tableType)
        }
      }
    }
    t
  }
}

// Leaf

object Start {
  def fromRecords(records: LynxRecords)(implicit context: LynxPlannerContext): Start = {
    Start(context.session.emptyGraphName, Some(records))
  }

  def fromRecords(qgn: QualifiedGraphName, records: LynxRecords)(implicit context: LynxPlannerContext): Start = {
    Start(qgn, Some(records))(context)
  }
}

final case class Start(qgn: QualifiedGraphName, maybeRecords: Option[LynxRecords] = None)(implicit override val context: LynxPlannerContext)
  extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader =
    maybeRecords.map(_.header).getOrElse(RecordHeader.empty)

  override lazy val _table: LynxDataFrame =
    maybeRecords.map(_.table).getOrElse(session.unitDataFrame)

  override lazy val graph: LynxPropertyGraph = resolve(qgn)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val maybeReturnItems: Option[Seq[Var]] = None

  override def toString: String = {
    val graphArg = qgn.toString
    val recordsArg = maybeRecords.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(", ")
    s"Start($allArgs)"
  }
}

// Unary
final case class PrefixGraph(in: PhysicalOperator, prefix: GraphIdPrefix) extends PhysicalOperator {

  override lazy val graphName: QualifiedGraphName = QualifiedGraphName(s"${in.graphName}_tempPrefixed_$prefix")

  override lazy val graph: LynxPropertyGraph = LynxPropertyGraph.prefixed(in.graph, prefix)
}

/**
 * Cache is a marker operator that indicates that its child operator is used multiple times within the query.
 */
final case class Cache(in: PhysicalOperator) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = in._table.cache()

}

final case class SwitchContext(in: PhysicalOperator, override val context: LynxPlannerContext) extends PhysicalOperator


final case class Alias(in: PhysicalOperator, aliases: Seq[AliasExpr]) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = in.recordHeader.withAlias(aliases: _*)
}

final case class Add(in: PhysicalOperator, exprs: List[Expr]) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = {
    exprs.foldLeft(in.recordHeader) { case (aggHeader, expr) =>
      if (aggHeader.contains(expr)) {
        expr match {
          case a: AliasExpr => aggHeader.withAlias(a)
          case _ => aggHeader
        }
      } else {
        expr match {
          case a: AliasExpr => aggHeader.withExpr(a.expr).withAlias(a)
          case _ => aggHeader.withExpr(expr)
        }
      }
    }
  }

  override lazy val _table: LynxDataFrame = {
    // TODO check for equal nullability setting
    val physicalAdditions = exprs.filterNot(in.recordHeader.contains)
    if (physicalAdditions.isEmpty) {
      in.table
    } else {
      in.table.withColumns(
        physicalAdditions.map(
          expr =>
            expr -> recordHeader.column(expr)): _*
      )(recordHeader, context.parameters)
    }
  }
}

final case class AddInto(in: PhysicalOperator, valueIntoTuples: List[(Expr, Expr)]) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = {
    valueIntoTuples.map(_._2).foldLeft(in.recordHeader)(_.withExpr(_))
  }

  override lazy val _table: LynxDataFrame = {
    val valuesToColumnNames = valueIntoTuples.map { case (value, into) => value -> recordHeader.column(into) }
    in.table.withColumns(valuesToColumnNames: _*)(recordHeader, context.parameters)
  }
}

final case class Drop[E <: Expr](in: PhysicalOperator, exprs: Set[E]) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = in.recordHeader -- exprs

  private lazy val columnsToDrop = in.recordHeader.columns -- recordHeader.columns

  override lazy val _table: LynxDataFrame = {
    if (columnsToDrop.nonEmpty) {
      in.table.drop(columnsToDrop.toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class Filter(in: PhysicalOperator, expr: Expr) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = in.table.filter(expr)(recordHeader, context.parameters)
}

final case class ReturnGraph(in: PhysicalOperator)
  extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = RecordHeader.empty

  override lazy val _table: LynxDataFrame = session.emptyDataFrame()
}

final case class Select(in: PhysicalOperator,
                        expressions: List[Expr],
                        columnRenames: Map[Expr, String] = Map.empty
                       ) extends PhysicalOperator {

  private lazy val selectHeader = in.recordHeader.select(expressions: _*)

  override lazy val recordHeader: RecordHeader = selectHeader.withColumnsReplaced(columnRenames)

  private lazy val returnExpressions = expressions.map {
    case AliasExpr(_, alias) => alias
    case other => other
  }

  override lazy val _table: LynxDataFrame = {
    val selectExpressions = returnExpressions.flatMap(expr => recordHeader.expressionsFor(expr).toSeq.sorted)
    val selectColumns = selectExpressions.map { expr => selectHeader.column(expr) -> recordHeader.column(expr) }.distinct
    in.table.select(selectColumns.head, selectColumns.tail: _*)
  }

  override lazy val maybeReturnItems: Option[Seq[Var]] =
    Some(returnExpressions.flatMap(_.owner).collect { case e: Var => e }.distinct)
}

final case class Distinct(
                           in: PhysicalOperator,
                           fields: Set[Var]
                         ) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = in.table.distinct(fields.flatMap(recordHeader.expressionsFor).map(recordHeader.column).toSeq: _*)

}

final case class Aggregate(
                            in: PhysicalOperator,
                            group: Set[Var],
                            aggregations: Set[(Var, Aggregator)]
                          ) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = in.recordHeader.select(group).withExprs(aggregations.map { case (v, _) => v })

  override lazy val _table: LynxDataFrame = {
    val preparedAggregations = aggregations.map { case (v, agg) => recordHeader.column(v) -> agg }.toMap
    in.table.group(group, preparedAggregations)(in.recordHeader, context.parameters)
  }
}

final case class OrderBy(
                          in: PhysicalOperator,
                          sortItems: Seq[SortItem]
                        ) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = {
    val tableSortItems = sortItems.map {
      case Asc(expr) => expr -> Ascending
      case Desc(expr) => expr -> Descending
    }
    in.table.orderBy(tableSortItems: _*)(recordHeader, context.parameters)
  }
}

final case class Skip(
                       in: PhysicalOperator,
                       expr: Expr
                     ) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }
    in.table.skip(skip)
  }
}

final case class Limit(
                        in: PhysicalOperator,
                        expr: Expr
                      ) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal", other)
    }
    in.table.limit(limit)
  }
}

final case class EmptyRecords(
                               in: PhysicalOperator,
                               fields: Set[Var] = Set.empty
                             ) extends PhysicalOperator {

  override lazy val recordHeader: RecordHeader = RecordHeader.from(fields)

  override lazy val _table: LynxDataFrame = session.emptyDataFrame()
}

final case class FromCatalogGraph(
                                   in: PhysicalOperator,
                                   logicalGraph: LogicalCatalogGraph
                                 ) extends PhysicalOperator {

  override def graph: LynxPropertyGraph = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}

// Binary

final case class Join(
                       lhs: PhysicalOperator,
                       rhs: PhysicalOperator,
                       joinExprs: Seq[(Expr, Expr)] = Seq.empty,
                       joinType: JoinType
                     ) extends PhysicalOperator {
  require((lhs.recordHeader.expressions intersect rhs.recordHeader.expressions).isEmpty, "Join cannot join operators with overlapping expressions")
  require((lhs.recordHeader.columns intersect rhs.recordHeader.columns).isEmpty, "Join cannot join tables with column name collisions")

  override lazy val recordHeader: RecordHeader = lhs.recordHeader join rhs.recordHeader

  override lazy val _table: LynxDataFrame = {
    val joinCols = joinExprs.map {
      case (l, r) =>
        recordHeader.column(l) -> rhs.recordHeader.column(r)
    }

    lhs.table.join(rhs.table, joinType, joinCols: _*)
  }
}

/**
 * Computes the union of the two input operators. The two inputs must have identical headers.
 * This operation does not remove duplicates.
 *
 * The output header of this operation is identical to the input headers.
 *
 * @param lhs the first operand
 * @param rhs the second operand
 */
// TODO: rename to UnionByName
// TODO: refactor to n-ary operator (i.e. take List[PhysicalOperator] as input)
final case class TabularUnionAll(
                                  lhs: PhysicalOperator,
                                  rhs: PhysicalOperator
                                ) extends PhysicalOperator {

  override lazy val _table: LynxDataFrame = {
    val lhsTable = lhs.table
    val rhsTable = rhs.table

    val leftColumns = lhsTable.physicalColumns
    val rightColumns = rhsTable.physicalColumns

    val sortedLeftColumns = leftColumns.sorted.mkString(", ")
    val sortedRightColumns = rightColumns.sorted.mkString(", ")

    if (leftColumns.size != rightColumns.size) {
      throw IllegalArgumentException("same number of columns", s"left:  $sortedLeftColumns\n\tright: $sortedRightColumns")
    }

    if (leftColumns.toSet != rightColumns.toSet) {
      throw IllegalArgumentException("same column names", s"left:  $sortedLeftColumns\n\tright: $sortedRightColumns")
    }

    val orderedRhsTable: LynxDataFrame = if (leftColumns != rightColumns) {
      rhsTable.select(leftColumns: _*)
    } else {
      rhsTable
    }

    lhsTable.unionAll(orderedRhsTable)
  }
}

final case class ConstructGraph(
                                 in: PhysicalOperator,
                                 constructedGraph: LynxPropertyGraph,
                                 construct: LogicalPatternGraph,
                                 override val context: LynxPlannerContext
                               ) extends PhysicalOperator {

  override def maybeReturnItems: Option[Seq[Var]] = None

  override lazy val recordHeader: RecordHeader = RecordHeader.empty

  override lazy val _table: LynxDataFrame = session.unitDataFrame

  override lazy val graph: LynxPropertyGraph = constructedGraph

  override def graphName: QualifiedGraphName = construct.qualifiedGraphName

  override def toString: String = {
    val elements = construct.clones.keySet ++ construct.newElements.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], elements=[${elements.mkString(", ")}])"
  }
}

// N-ary

final case class GraphUnionAll(
                                inputs: NonEmptyList[PhysicalOperator],
                                qgn: QualifiedGraphName
                              ) extends PhysicalOperator {

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val graph: LynxPropertyGraph = LynxPropertyGraph.union(inputs.map(_.graph).toList: _*)
}