package org.grapheco.lynx.dataframe


import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

trait DataFrameOperator {
  def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame

  def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame

  def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame

  def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame

  def skip(df: DataFrame, num: Int): DataFrame

  def take(df: DataFrame, num: Int): DataFrame

  def join(a: DataFrame, b: DataFrame, joinColumn: Seq[String], joinType: JoinType): DataFrame

  def distinct(df: DataFrame): DataFrame

  def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame
}

sealed trait JoinType

object InnerJoin extends JoinType
object OuterJoin extends JoinType
object LeftJoin  extends JoinType
object RightJoin extends JoinType