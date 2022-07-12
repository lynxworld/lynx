package org.grapheco.lynx.dataframe

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

trait DataFrame {
  def schema: Seq[(String, LynxType)]

  def columnsName: Seq[String] = schema.map(_._1)

  def records: Iterator[Seq[LynxValue]]
}

object DataFrame {
  def empty: DataFrame = DataFrame(Seq.empty, () => Iterator.empty)

  def apply(schema0: Seq[(String, LynxType)], records0: () => Iterator[Seq[LynxValue]]): DataFrame =
    new DataFrame {
      override def schema: Seq[(String, LynxType)] = schema0

      override def records: Iterator[Seq[LynxValue]] = records0()
    }

  def cached(schema0: Seq[(String, LynxType)], records: Seq[Seq[LynxValue]]): DataFrame =
    apply(schema0, () => records.iterator)

  def unit(columns: Seq[(String, Expression)])(implicit expressionEvaluator: ExpressionEvaluator, ctx: ExpressionContext): DataFrame = {
    val schema = columns.map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, Map.empty)
    )

    DataFrame(schema, () => Iterator.single(
      columns.map(col => {
        expressionEvaluator.eval(col._2)(ctx)
      })))
  }

  def updateColumns(colIndexs: Seq[Int], newColsValues: Seq[Iterator[LynxValue]], srcDF: DataFrame): DataFrame = {
    if(colIndexs.length != newColsValues.length) throw new Exception(s"Lengths of colIndexs and newColsValues are not equal.")
    if (colIndexs.length * newColsValues.length == 0) throw new Exception(s"Length of colIndexs or newColsValues is 0.")
    val colIndex: Int = colIndexs.head
    val newColValues: Iterator[LynxValue] = newColsValues.head
    val updatedDF: DataFrame = DataFrame(srcDF.schema, () => srcDF.records.zip(newColValues).map{
      case (row, newValue) =>
        row.updated(colIndex, newValue)
    })
    if (colIndexs.length == 1) updatedDF
    else {
      updateColumns(colIndexs.drop(1), newColsValues.drop(1), updatedDF)
    }
  }
}