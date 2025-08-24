package org.grapheco.lynx.dataframe

import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.types.{LynxType, LynxValue}
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.Expression

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:25 2022/7/5
 * @Modified By:
 */
class DefaultDataFrameOperator(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {

  private def prepareColumns(df: DataFrame, columns: Seq[(String, Option[String])]): (Map[String, LynxType], Map[String, Int], Seq[Int]) = {
      val sourceSchema = df.schema.toMap
      val columnNameIndex = df.columnsName.zipWithIndex.toMap
      val usedIndices = columns.map(_._1).map(columnNameIndex)
      (sourceSchema, columnNameIndex, usedIndices)
    }
  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = {
    val (sourceSchema, _, usedIndices) = prepareColumns(df, columns)
    val newSchema = columns.map(col => col._2.getOrElse(col._1) -> sourceSchema(col._1))

    DataFrame(newSchema, () => df.records.map(row => usedIndices.map(row.apply)))
  }

  override def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame =
    DataFrame(df.schema, () => df.records.filter(predicate))

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val newSchema: Seq[(String, LynxType)] = columns.map {
      case (name, expression) => name -> expressionEvaluator.typeOf(expression, df.schema.toMap)
    }

    DataFrame(newSchema,
      () => df.records.map(
        record => {
          val recordCtx = ctx.withVars(df.columnsName.zip(record).toMap)
          columns.map(col => expressionEvaluator.eval(col._2)(recordCtx)) //TODO: to opt
        }
      )
    )
  }


  override def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val newSchema = (groupings ++ aggregations).map(col => col._1 -> expressionEvaluator.typeOf(col._2, df.schema.toMap))
    val columnsName = df.columnsName

    DataFrame(newSchema, () => {
      if (groupings.nonEmpty) {
        df.records.map { record =>
          val recordCtx = ctx.withVars(columnsName.zip(record).toMap)
          val groupingValues = groupings.map(col => expressionEvaluator.eval(col._2)(recordCtx))
          groupingValues -> recordCtx
        }.groupBy(_._1)
        .view.flatMap { case (groupingValue, records) =>
          Iterator(groupingValue ++ aggregations.map { case (_, expr) =>
            expressionEvaluator.aggregateEval(expr)(records.map(_._2))
          })
        }.iterator
      } else {
        val allRecordsCtx = df.records.map(record => ctx.withVars(columnsName.zip(record).toMap)).toSeq
        Iterator(aggregations.map { case (_, expr) => expressionEvaluator.aggregateEval(expr)(allRecordsCtx) })
      }
    })
  }

  override def skip(df: DataFrame, num: Int): DataFrame =
    DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.take(num))

  override def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    // Select the connection algorithm based on heuristic rules
    JoinerSelector.chooseJoiner(a, b, joinColumns, joinType)(a,b,joinColumns,joinType)
  }

  override def cross(a: DataFrame, b: DataFrame): DataFrame = {
    DataFrame(a.schema ++ b.schema, () => a.records.flatMap(ra => b.records.map(ra ++ _)))
  }

  /*
  * @param: df is a DataFrame
  * @function: Remove the duplicated rows in the df.
  * */
  override def distinct(df: DataFrame): DataFrame = DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = {
    val columnsName = df.columnsName
    val sortedRecords = df.records.toSeq.sortWith { (A, B) =>
      val ctxA = ctx.withVars(columnsName.zip(A).toMap)
      val ctxB = ctx.withVars(columnsName.zip(B).toMap)
      val sortValues = sortItem.map { case (exp, asc) =>
        val valueA = expressionEvaluator.eval(exp)(ctxA)
        val valueB = expressionEvaluator.eval(exp)(ctxB)
        (valueA.compareTo(valueB), asc)
      }
      sortValues.exists { case (cmp, asc) => cmp != 0 && (cmp > 0) == asc }
    }
    DataFrame(df.schema, () => sortedRecords.iterator)
  }

  private def _ascCmp(sortValue: Iterator[(LynxValue, LynxValue, Boolean)]): Boolean = {
    sortValue.find { case (valueOfA, valueOfB, asc) =>
      val comparable = valueOfA.compareTo(valueOfB)
      comparable != 0 && comparable > 0 != asc
    }.isDefined
  }

}
