package org.opencypher.lynx

import org.opencypher.lynx.plan.{JoinType, Order}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.ir.api.expr.{Aggregator, CountStar, Expr, Var}

import scala.collection.Seq

case class EvalContext(header: RecordHeader, valueOfColumn: (String) => CypherValue, parameters: CypherMap) {

}

trait TableOperator {
  def aggregate(expr: Expr, values: Seq[CypherValue]): CypherValue

  def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue

  /**
   * If supported by the backend, calling that operator caches the underlying table within the backend runtime.
   *
   * @return cached version of that table
   */
  def cache(table: LynxTable): LynxTable

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select
   * @return table containing only requested columns
   */
  def select(table: LynxTable, cols: String*): LynxTable

  /**
   * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
   *
   * @param cols columns to select and their alias
   * @return table containing only requested aliased columns
   */
  def selectWithAlias(table: LynxTable, cols: (String, String)*): LynxTable

  /**
   * Returns a table containing only rows where the given expression evaluates to true.
   *
   * @param expr       filter expression
   * @param header     table record header
   * @param parameters query parameters
   * @return table with filtered rows
   */
  def filter(table: LynxTable, expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): LynxTable

  /**
   * Returns a table with the given columns removed.
   *
   * @param cols columns to drop
   * @return table with dropped columns
   */
  def drop(table: LynxTable, cols: String*): LynxTable

  /**
   * Joins the current table with the given table on the specified join columns using equi-join semantics.
   *
   * @param a        table to join
   * @param b        table to join
   * @param joinType join type to perform (e.g. inner, outer, ...)
   * @param joinCols columns to join the two tables on
   * @return joined table
   */
  def join(a: LynxTable, b: LynxTable, joinType: JoinType, joinCols: (String, String)*): LynxTable

  /**
   * Computes the union of the current table and the given table. Requires both tables to have identical column layouts.
   *
   * @param a table to join
   * @param b table to join
   * @return union table
   */
  def unionAll(a: LynxTable, b: LynxTable): LynxTable

  /**
   * Returns a table that is ordered by the given columns.
   *
   * @param sortItems a sequence of column names and their order (i.e. ascending / descending)
   * @return ordered table
   */
  def orderBy(table: LynxTable, sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): LynxTable

  /**
   * Returns a table without the first n rows of the current table.
   *
   * @param n number of rows to skip
   * @return table with skipped rows
   */
  def skip(table: LynxTable, n: Long): LynxTable

  /**
   * Returns a table containing the first n rows of the current table.
   *
   * @param n number of rows to return
   * @return table with at most n rows
   */
  def limit(table: LynxTable, n: Long): LynxTable

  /**
   * Returns a table where each row is unique with regard to the specified columns
   *
   * @param cols columns to consider when comparing rows
   * @return table containing the specific columns and distinct rows
   */
  def distinct(table: LynxTable, cols: String*): LynxTable

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
  def group(table: LynxTable, by: Set[Var], aggregations: Map[String, Aggregator])
           (implicit header: RecordHeader, parameters: CypherMap): LynxTable

  /**
   * Returns a table with additional expressions, which are evaluated and stored in the specified columns.
   *
   * @note If the column already exists, its contents will be replaced.
   * @param columns    tuples of expressions to evaluate and corresponding column name
   * @param header     table record header
   * @param parameters query parameters
   * @return
   */
  def withColumns(table: LynxTable, columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): LynxTable

  /**
   * Prints the table to the system console.
   *
   * @param rows number of rows to print
   */
  def show(table: LynxTable, rows: Int = 20): Unit
}
