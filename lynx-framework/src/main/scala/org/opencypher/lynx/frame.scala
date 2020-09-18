package org.opencypher.lynx

import org.opencypher.lynx.planning.{InnerJoin, JoinType, Order}
import org.opencypher.lynx.util.{EvalContext, ExpressionEvaluator}
import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types.{CTIdentity, CTNode, CTRelationship, CTVoid, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.{PrintOptions, TablePrinter}
import org.opencypher.okapi.ir.api.{Label, RelType}
import org.opencypher.okapi.ir.api.expr.{Aggregator, AliasExpr, ElementProperty, EndNode, Expr, HasLabel, HasType, NodeVar, Property, RelationshipVar, ReturnItem, StartNode, Var}
import org.opencypher.okapi.api.value.CypherValue.Format._

import scala.annotation.tailrec

object LynxDataFrame {
  val unit = new LynxDataFrame(Map.empty[String, CypherType], Stream.apply(Map[String, CypherValue]()))
  val empty = new LynxDataFrame(Map.empty[String, CypherType], Stream.empty[Map[String, CypherValue]])
}

//meta: (name,STRING),(age,INTEGER)
case class LynxDataFrame(schema: Map[String, CypherType], records: Stream[Map[String, CypherValue]]) extends CypherTable {

  /**
   * If supported by the backend, calling that operator caches the underlying table within the backend runtime.
   *
   * @return cached version of that table
   */
  def cache() = this

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select
   * @return table containing only requested columns
   */
  def select(cols: String*): LynxDataFrame = {
    val tuples = cols.zip(cols)
    select(tuples.head, tuples.tail: _*)
  }

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select and their alias
   * @return table containing only requested aliased columns
   */
  def select(col: (String, String), cols: (String, String)*): LynxDataFrame = {
    val columns = col +: cols

    LynxDataFrame(columns.map(column =>
      column._2 -> schema(column._1)).toMap,
      records.map(record =>
        columns.map(column => column._2 -> record(column._1)).toMap)
    )
  }

  /**
   * Returns a table containing only rows where the given expression evaluates to true.
   *
   * @param expr       filter expression
   * @param header     table record header
   * @param parameters query parameters
   * @return table with filtered rows
   */
  def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame = {
    LynxDataFrame(schema,
      records.filter { map =>
        implicit val ctx = EvalContext(header, map, parameters)
        ExpressionEvaluator.eval(expr) match {
          case CypherNull => false
          case CypherBoolean(x) => x
        }
      })
  }

  /**
   * Returns a table with the given columns removed.
   *
   * @param cols columns to drop
   * @return table with dropped columns
   */
  def drop(cols: String*): LynxDataFrame = {
    LynxDataFrame(schema -- cols, records.map(_ -- cols))
  }

  /**
   * Joins the current table with the given table on the specified join columns using equi-join semantics.
   *
   * @param other    table to join
   * @param joinType join type to perform (e.g. inner, outer, ...)
   * @param joinCols columns to join the two tables on
   * @return joined table
   */
  def join(other: LynxDataFrame, joinType: JoinType, joinCols: (String, String)*): LynxDataFrame = {
    joinType match {
      case InnerJoin => {
        val joined = this.records.flatMap {
          a => {
            other.records.filter { b => {
              joinCols.map(kv => a(kv._1) == b(kv._2)).reduce(_ && _)
            }
            }.map {
              b =>
                a ++ b
            }
          }
        }

        LynxDataFrame(this.schema ++ other.schema, joined)
      }
    }
  }

  /**
   * Computes the union of the current table and the given table. Requires both tables to have identical column layouts.
   *
   * @param other table to union with
   * @return union table
   */
  def unionAll(other: LynxDataFrame): LynxDataFrame = {
    LynxDataFrame(this.schema ++ other.schema, this.records.union(other.records))
  }

  /**
   * Returns a table that is ordered by the given columns.
   *
   * @param sortItems a sequence of column names and their order (i.e. ascending / descending)
   * @return ordered table
   */
  def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame = {
    ???
  }

  /**
   * Returns a table without the first n rows of the current table.
   *
   * @param n number of rows to skip
   * @return table with skipped rows
   */
  def skip(n: Long): LynxDataFrame = {
    LynxDataFrame(this.schema, this.records.drop(n.toInt))
  }

  /**
   * Returns a table containing the first n rows of the current table.
   *
   * @param n number of rows to return
   * @return table with at most n rows
   */
  def limit(n: Long): LynxDataFrame = {
    LynxDataFrame(this.schema, this.records.take(n.toInt))
  }

  /**
   * Returns a table where each row is unique.
   *
   * @return table with unique rows
   */
  def distinct: LynxDataFrame = {
    LynxDataFrame(this.schema, this.records.distinct)
  }

  /**
   * Returns a table where each row is unique with regard to the specified columns
   *
   * @param cols columns to consider when comparing rows
   * @return table containing the specific columns and distinct rows
   */
  def distinct(cols: String*): LynxDataFrame = {
    this.select(cols: _*).distinct
  }

  /**
   * Groups the rows within the table by the given query variables. Additionally a set of aggregations can be performed
   * on the grouped table.
   *
   * @param by           query variables to group by (e.g. (n)), if empty, the whole row is used as grouping key
   * @param aggregations set of aggregations functions and the column to store the result in
   * @param header       table record header
   * @param parameters   query parameters
   * @return table grouped by the given keys and results of possible aggregate functions
   */
  def group(by: Set[Var], aggregations: Map[String, Aggregator])
           (implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame = {
    val df2 = records.groupBy(map => by.map(map(_))).map(groupped => {
      aggregations.map(agr => agr._1 -> ExpressionEvaluator.aggregate(agr._2, groupped._2.map(_.apply(agr._1))))
    })

    LynxDataFrame(aggregations.map(agr => agr._1 -> schema(agr._1)), df2.toStream)
  }

  /**
   * Returns a table with additional expressions, which are evaluated and stored in the specified columns.
   *
   * @note If the column already exists, its contents will be replaced.
   * @param columns    tuples of expressions to evaluate and corresponding column name
   * @param header     table record header
   * @param parameters query parameters
   * @return
   */
  def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame = {
    LynxDataFrame(schema ++ columns.map(column => column._2 -> column._1.cypherType),
      records.map(record =>
        record ++ columns.map(column => {
          implicit val ctx = EvalContext(header, record, parameters)
          column._2 -> ExpressionEvaluator.eval(column._1)
        })
      )
    )
  }

  /**
   * Prints the table to the system console.
   *
   * @param rows number of rows to print
   */
  def show(rows: Int = 20): Unit = {
    val columns = schema.keySet
    implicit val options: PrintOptions = PrintOptions.out
    val content: Seq[Seq[CypherValue]] = records.take(rows).map { row =>
      columns.foldLeft(Seq.empty[CypherValue]) {
        case (currentSeq, column) => currentSeq :+ row(column)
      }
    }

    options.stream
      .append(TablePrinter.toTable(columns.toSeq, content)(v => v.toCypherString))
      .flush()
  }

  override def physicalColumns: Seq[String] = schema.keySet.toSeq

  override def columnType: Map[String, CypherType] = schema

  override def rows: Iterator[String => CypherValue.CypherValue] =
    records.map(record => (key: String) => record(key)).iterator

  override def size: Long = records.size
}

object RecordHeader {

  def empty: RecordHeader = RecordHeader(Map.empty)

  def from[T <: Expr](expr: T, exprs: T*): RecordHeader = empty.withExprs(expr, exprs: _*)

  def from[T <: Expr](exprs: Set[T]): RecordHeader = empty.withExprs(exprs)

  def from[T <: Expr](exprs: Seq[T]): RecordHeader = from(exprs.head, exprs.tail: _*)

  def from(v: Var): RecordHeader = v.cypherType match {
    case CTNode(labels, _) =>
      from(
        v,
        labels.map(l => HasLabel(v, Label(l))).toSeq: _*
      )
    case CTRelationship(types, _) => from(
      v,
      Seq(StartNode(v)(CTIdentity), EndNode(v)(CTIdentity))
        ++ types.map(t => HasType(v, RelType(t))): _*
    )
    case other => throw IllegalArgumentException("A node or relationship variable", other)
  }
}

final case class RecordHeaderException(msg: String) extends RuntimeException(msg)

case class RecordHeader(exprToColumn: Map[Expr, String]) {

  // ==============
  // Lookup methods
  // ==============

  def expressions: Set[Expr] = exprToColumn.keySet

  def vars: Set[Var] = expressions.flatMap(_.owner).collect { case v: Var => v }

  def returnItems: Set[ReturnItem] = expressions.flatMap(_.owner).collect { case r: ReturnItem => r }

  def columns: Set[String] = exprToColumn.values.toSet

  def isEmpty: Boolean = exprToColumn.isEmpty

  // TODO: should we verify that if the expr exists, that it has the same type and nullability
  def contains(expr: Expr): Boolean = expr match {
    case AliasExpr(_, alias) => contains(alias)
    case _ => exprToColumn.contains(expr)
  }

  def getColumn(expr: Expr): Option[String] = exprToColumn.get(expr)

  def column(expr: Expr): String = expr match {
    case AliasExpr(innerExpr, _) => column(innerExpr)
    case _ => exprToColumn.getOrElse(expr, throw IllegalArgumentException(s"Header does not contain a column for $expr.\n\t${this.toString}"))
  }

  def ownedBy(expr: Var): Set[Expr] = {
    val members = exprToColumn.keys.filter(e => e.owner.contains(expr)).toSet

    members.flatMap {
      case e: Var if e == expr => Seq(e)
      case e: Var => ownedBy(e) + e
      case other => Seq(other)
    }
  }

  def expressionsFor(expr: Expr): Set[Expr] = {
    expr match {
      case v: Var => if (exprToColumn.contains(v)) ownedBy(v) + v else ownedBy(v)
      case e if exprToColumn.contains(e) => Set(e)
      case _ => Set.empty
    }
  }

  def expressionsFor(column: String): Set[Expr] = {
    exprToColumn.collect { case (k, v) if v == column => k }.toSet
  }

  def aliasesFor(expr: Expr): Set[Var] = {
    val aliasesFromHeader: Set[Var] = getColumn(expr) match {
      case None => Set.empty
      case Some(col) => exprToColumn.collect { case (k: Var, v) if v == col => k }.toSet
    }

    val aliasesFromParam: Set[Var] = expr match {
      case v: Var => Set(v)
      case _ => Set.empty
    }

    aliasesFromHeader ++ aliasesFromParam
  }

  // ===================
  // Convenience methods
  // ===================

  def idExpressions(): Set[Expr] = {
    exprToColumn.keySet.collect {
      case n if n.cypherType.subTypeOf(CTNode) => n
      case r if r.cypherType.subTypeOf(CTRelationship) => r
    }
  }

  def idExpressions(v: Var): Set[Expr] = idExpressions().filter(_.owner.get == v)

  def idColumns(): Set[String] = idExpressions().map(column)

  def idColumns(v: Var): Set[String] = idExpressions(v).map(column)

  def labelsFor(n: Var): Set[HasLabel] = {
    ownedBy(n).collect {
      case l: HasLabel => l
    }
  }

  def typesFor(r: Var): Set[HasType] = {
    ownedBy(r).collect {
      case t: HasType => t
    }
  }

  def startNodeFor(r: Var): StartNode = {
    ownedBy(r).collectFirst {
      case s: StartNode => s
    }.get
  }

  def endNodeFor(r: Var): EndNode = {
    ownedBy(r).collectFirst {
      case e: EndNode => e
    }.get
  }

  def propertiesFor(v: Var): Set[Property] = {
    ownedBy(v).collect {
      case p: Property => p
    }
  }

  def node(name: Var): Set[Expr] = {
    exprToColumn.keys.collect {
      case n: Var if name == n => n
      case h@HasLabel(n: Var, _) if name == n => h
      case p@ElementProperty(n: Var, _) if name == n => p
    }.toSet
  }

  def elementVars: Set[Var] = nodeVars[Var] ++ relationshipVars[Var]

  def nodeVars[T >: NodeVar <: Var]: Set[T] = {
    exprToColumn.keySet.collect {
      case v: NodeVar => v
    }
  }

  def nodeElements: Set[Var] = {
    exprToColumn.keySet.collect {
      case v: Var if v.cypherType.subTypeOf(CTNode) => v
    }
  }

  def relationshipVars[T >: RelationshipVar <: Var]: Set[T] = {
    exprToColumn.keySet.collect {
      case v: RelationshipVar => v
    }
  }

  def relationshipElements: Set[Var] = {
    exprToColumn.keySet.collect {
      case v: Var if v.cypherType.material.subTypeOf(CTRelationship) => v
    }
  }

  def elementsForType(ct: CypherType, exactMatch: Boolean = false): Set[Var] = {
    ct match {
      case n: CTNode => nodesForType(n, exactMatch)
      case r: CTRelationship => relationshipsForType(r)
      case other => throw IllegalArgumentException("Element", other)
    }
  }

  def nodesForType[T >: NodeVar <: Var](nodeType: CTNode, exactMatch: Boolean = false): Set[T] = {
    // and semantics
    val requiredLabels = nodeType.labels

    nodeVars[T].filter { nodeVar =>
      val physicalLabels = labelsFor(nodeVar).map(_.label.name)
      val logicalLabels = nodeVar.cypherType match {
        case CTNode(labels, _) => labels
        case _ => Set.empty[String]
      }
      if (exactMatch) {
        requiredLabels == (physicalLabels ++ logicalLabels)
      } else {
        requiredLabels.subsetOf(physicalLabels ++ logicalLabels)
      }
    }
  }

  def relationshipsForType[T >: RelationshipVar <: Var](relType: CTRelationship): Set[T] = {
    // or semantics
    val possibleTypes = relType.types

    relationshipVars[T].filter { relVar =>
      val physicalTypes = typesFor(relVar).map {
        case HasType(_, RelType(name)) => name
      }
      val logicalTypes = relVar.cypherType match {
        case CTRelationship(types, _) => types
        case _ => Set.empty[String]
      }
      possibleTypes.isEmpty || (physicalTypes ++ logicalTypes).exists(possibleTypes.contains)
    }
  }

  // ================
  // Mutation methods
  // ================

  def select[T <: Expr](exprs: T*): RecordHeader = select(exprs.toSet)

  def select[T <: Expr](exprs: Set[T]): RecordHeader = {
    val aliasExprs = exprs.collect { case a: AliasExpr => a }
    val headerWithAliases = withAlias(aliasExprs.toSeq: _*)
    val selectExpressions = exprs.flatMap { e: Expr =>
      e match {
        case v: Var => expressionsFor(v)
        case AliasExpr(expr, alias) =>
          expr match {
            case v: Var => expressionsFor(v).map(_.withOwner(alias))
            case other => other.withOwner(alias)
          }
        case nonVar => Set(nonVar)
      }
    }
    val selectMappings = headerWithAliases.exprToColumn.filterKeys(selectExpressions.contains)
    RecordHeader(selectMappings)
  }

  def withColumnsReplaced[T <: Expr](replacements: Map[T, String]): RecordHeader = {
    RecordHeader(exprToColumn ++ replacements)
  }

  def withColumnsRenamed[T <: Expr](renames: Seq[(T, String)]): RecordHeader = {
    renames.foldLeft(this) {
      case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
    }
  }

  def withColumnRenamed[T <: Expr](expr: T, newColumn: String): RecordHeader = {
    withColumnRenamed(column(expr), newColumn)
  }

  def withColumnRenamed(oldColumn: String, newColumn: String): RecordHeader = {
    val exprs = expressionsFor(oldColumn)
    copy(exprToColumn ++ exprs.map(_ -> newColumn))
  }

  private[opencypher] def newConflictFreeColumnName(expr: Expr, usedColumnNames: Set[String] = columns): String = {
    @tailrec def recConflictFreeColumnName(candidateName: String): String = {
      if (usedColumnNames.contains(candidateName)) recConflictFreeColumnName(s"_$candidateName")
      else candidateName
    }

    val firstColumnNameCandidate = expr.toString
      .replaceAll("-", "_")
      .replaceAll(":", "_")
      .replaceAll("\\.", "_")
    recConflictFreeColumnName(firstColumnNameCandidate)
  }

  def withExpr(expr: Expr): RecordHeader = {
    expr match {
      case a: AliasExpr => withAlias(a)
      case _ => exprToColumn.get(expr) match {
        case Some(_) => this

        case None =>
          val newColumnName = newConflictFreeColumnName(expr)

          // Aliases for (possible) owner of expr need to be updated as well
          val exprsToAdd: Set[Expr] = expr.owner match {
            case None => Set(expr)

            case Some(exprOwner) => aliasesFor(exprOwner).map(alias => expr.withOwner(alias))
          }

          exprsToAdd.foldLeft(this) {
            case (current, e) => current.addExprToColumn(e, newColumnName)
          }
      }
    }
  }

  def withExprs[T <: Expr](expr: T, exprs: T*): RecordHeader = (expr +: exprs).foldLeft(this)(_ withExpr _)

  def withExprs[T <: Expr](exprs: Set[T]): RecordHeader = {
    if (exprs.isEmpty) {
      this
    } else {
      withExprs(exprs.head, exprs.tail.toSeq: _*)
    }
  }

  def withAlias(aliases: AliasExpr*): RecordHeader = aliases.foldLeft(this) {
    case (currentHeader, alias) => currentHeader.withAlias(alias)
  }

  def withAlias(expr: AliasExpr): RecordHeader = {
    val to = expr.expr
    val alias = expr.alias
    to match {
      // Element case
      case elementExpr: Var if exprToColumn.contains(to) =>
        val withElementExpr = addExprToColumn(alias, exprToColumn(to))
        ownedBy(elementExpr).filterNot(_ == elementExpr).foldLeft(withElementExpr) {
          case (current, nextExpr) => current.addExprToColumn(nextExpr.withOwner(alias), exprToColumn(nextExpr))
        }

      // Non-element case
      case e if exprToColumn.contains(e) => addExprToColumn(alias, exprToColumn(e))

      // No expression to alias
      case other => throw IllegalArgumentException(s"An expression in $this", s"Unknown expression $other")
    }
  }

  def join(other: RecordHeader): RecordHeader = {
    val expressionOverlap = expressions.intersect(other.expressions)
    if (expressionOverlap.nonEmpty) {
      throw IllegalArgumentException("two headers with non overlapping expressions", s"overlapping expressions: $expressionOverlap")
    }

    val columnOverlap = columns intersect other.columns
    if (columnOverlap.nonEmpty) {
      throw IllegalArgumentException("two headers with non overlapping columns", s"overlapping columns: $columnOverlap")
    }

    this ++ other
  }

  def union(other: RecordHeader): RecordHeader = {
    val varOverlap = vars ++ other.vars
    if (varOverlap != vars) {
      throw IllegalArgumentException("two headers with the same variables", s"$vars and ${other.vars}")
    }

    this ++ other
  }

  def ++(other: RecordHeader): RecordHeader = {
    val result = (exprToColumn ++ other.exprToColumn).map {
      case (key, value) =>
        val leftCT = exprToColumn.keySet.find(_ == key).map(_.cypherType).getOrElse(CTVoid)
        val rightCT = other.exprToColumn.keySet.find(_ == key).map(_.cypherType).getOrElse(CTVoid)

        val resultExpr = (key, leftCT, rightCT) match {
          case (v: Var, l: CTNode, r: CTNode) => Var(v.name)(l.join(r))
          case (v: Var, l: CTRelationship, r: CTRelationship) => Var(v.name)(l.join(r))
          case (_, l, r) if l.subTypeOf(r) => other.exprToColumn.keySet.collectFirst { case k if k == key => k }.get
          case (_, l, r) if r.subTypeOf(l) => key
          case _ => throw IllegalArgumentException(
            expected = s"Compatible Cypher types for expression $key",
            actual = s"left type `$leftCT` and right type `$rightCT`"
          )
        }
        resultExpr -> value
    }
    copy(exprToColumn = result)
  }

  def --[T <: Expr](expressions: Set[T]): RecordHeader = {
    val expressionToRemove = expressions.flatMap(expressionsFor)
    val updatedExprToColumn = exprToColumn.filterNot { case (e, _) => expressionToRemove.contains(e) }
    copy(exprToColumn = updatedExprToColumn)
  }

  def addExprToColumn(expr: Expr, columnName: String): RecordHeader = {
    // We need to possibly remove the existing expression in order to update the CypherType
    copy(exprToColumn = exprToColumn - expr + (expr -> columnName))
  }

  override def toString: String = exprToColumn.keys
    .map(_.withoutType)
    .map(e => s"`$e`")
    .toSeq
    .sorted
    .mkString("[", ", ", "]")

  def pretty: String = {
    val formatCell: String => String = s => s"'$s'"
    val (header, row) = exprToColumn
      .toSeq
      .sortBy(_._2)
      .map { case (expr, column) => expr.toString -> column }
      .unzip
    TablePrinter.toTable(header, Seq(row))(formatCell)
  }

  def show(): Unit = println(pretty)

}
