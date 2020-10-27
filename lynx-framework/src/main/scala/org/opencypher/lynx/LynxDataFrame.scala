package org.opencypher.lynx

import org.opencypher.lynx.planning.{InnerJoin, JoinType, Order}
import org.opencypher.lynx.util.EvalContext
import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.impl.util.{PrintOptions, TablePrinter}
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}

import scala.collection.Seq

trait LynxDataFrame extends CypherTable {

  /**
   * If supported by the backend, calling that operator caches the underlying table within the backend runtime.
   *
   * @return cached version of that table
   */
  def cache():this.type

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select
   * @return table containing only requested columns
   */
  def select(cols: String*): this.type

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select and their alias
   * @return table containing only requested aliased columns
   */
  def select(col: (String, String), cols: (String, String)*): this.type

  /**
   * Returns a table containing only rows where the given expression evaluates to true.
   *
   * @param expr       filter expression
   * @param header     table record header
   * @param parameters query parameters
   * @return table with filtered rows
   */
  def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): this.type

  /**
   * Returns a table with the given columns removed.
   *
   * @param cols columns to drop
   * @return table with dropped columns
   */
  def drop(cols: String*): this.type

  /**
   * Joins the current table with the given table on the specified join columns using equi-join semantics.
   *
   * @param other    table to join
   * @param joinType join type to perform (e.g. inner, outer, ...)
   * @param joinCols columns to join the two tables on
   * @return joined table
   */
  def join(other: LynxDataFrame, joinType: JoinType, joinCols: (String, String)*): this.type

  /**
   * Computes the union of the current table and the given table. Requires both tables to have identical column layouts.
   *
   * @param other table to union with
   * @return union table
   */
  def unionAll(other: LynxDataFrame): this.type

  /**
   * Returns a table that is ordered by the given columns.
   *
   * @param sortItems a sequence of column names and their order (i.e. ascending / descending)
   * @return ordered table
   */
  def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): this.type

  /**
   * Returns a table without the first n rows of the current table.
   *
   * @param n number of rows to skip
   * @return table with skipped rows
   */
  def skip(n: Long): this.type

  /**
   * Returns a table containing the first n rows of the current table.
   *
   * @param n number of rows to return
   * @return table with at most n rows
   */
  def limit(n: Long): this.type
  /**
   * Returns a table where each row is unique.
   *
   * @return table with unique rows
   */
  def distinct: this.type

  /**
   * Returns a table where each row is unique with regard to the specified columns
   *
   * @param cols columns to consider when comparing rows
   * @return table containing the specific columns and distinct rows
   */
  def distinct(cols: String*):this.type

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
           (implicit header: RecordHeader, parameters: CypherMap):this.type

  /**
   * Returns a table with additional expressions, which are evaluated and stored in the specified columns.
   *
   * @note If the column already exists, its contents will be replaced.
   * @param columns    tuples of expressions to evaluate and corresponding column name
   * @param header     table record header
   * @param parameters query parameters
   * @return
   */
  def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): this.type

  /**
   * Prints the table to the system console.
   *
   * @param rows number of rows to print
   */
  def show(rows: Int = 20): Unit
}
