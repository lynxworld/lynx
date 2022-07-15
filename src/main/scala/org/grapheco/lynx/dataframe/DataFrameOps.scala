package org.grapheco.lynx.dataframe

import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

trait DataFrameOps {
  val srcFrame: DataFrame
  val operator: DataFrameOperator

  def select(columns: Seq[(String, Option[String])]): DataFrame = operator.select(srcFrame, columns)

  def project(columns: Seq[(String, Expression)])(implicit ctx: ExpressionContext): DataFrame =
    operator.project(srcFrame, columns)(ctx)

  def groupBy(groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(implicit ctx: ExpressionContext): DataFrame =
    operator.groupBy(srcFrame, groupings, aggregations)(ctx)

  def join(b: DataFrame, isSingleMatch: Boolean, joinType: JoinType): DataFrame = {
    // Warning
    val commonColNames: Seq[String] = srcFrame.columnsName.filter(srcColName => b.columnsName.contains(srcColName))
    operator.join(srcFrame, b, commonColNames, joinType)
  }

  def filter(predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame = operator.filter(srcFrame, predicate)(ctx)

  def take(num: Int): DataFrame = operator.take(srcFrame, num)

  def skip(num: Int): DataFrame = operator.skip(srcFrame, num)

  def distinct(): DataFrame = operator.distinct(srcFrame)

  /**
   *
   * @param sortItem Seq[(Expression, Asc?)]
   * @param ctx
   * @return
   */
  def orderBy(sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = operator.orderBy(srcFrame, sortItem)(ctx)
}

object DataFrameOps {
  implicit def ops(ds: DataFrame)(implicit dfo: DataFrameOperator): DataFrameOps = DataFrameOps(ds)(dfo)

  def apply(ds: DataFrame)(dfo: DataFrameOperator): DataFrameOps = new DataFrameOps {
    override val srcFrame: DataFrame = ds
    val operator: DataFrameOperator = dfo
  }
}