package org.grapheco.lynx

import org.grapheco.lynx.DataFrame.typeOf
import org.opencypher.v9_0.expressions.{BooleanLiteral, DoubleLiteral, Expression, IntegerLiteral, Parameter, Property, StringLiteral, True, Variable}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

trait DataFrame {
  def schema: Seq[(String, LynxType)]

  def records: Iterator[Seq[LynxValue]]
}

object DataFrame {
  def empty: DataFrame = DataFrame(Seq.empty, () => Iterator.empty)

  def typeOf(expr: Expression, varTypes: Map[String, LynxType]): LynxType =
    expr match {
      case Parameter(name, parameterType) => parameterType

      //literal
      case _: BooleanLiteral => CTBoolean
      case _: StringLiteral => CTString
      case _: IntegerLiteral => CTInteger
      case _: DoubleLiteral => CTFloat

      case Variable(name) => varTypes(name)
      case _ => CTAny
    }

  def apply(schema0: Seq[(String, LynxType)], records0: () => Iterator[Seq[LynxValue]]) =
    new DataFrame {
      override def schema = schema0

      override def records = records0()
    }

  def unit(columns: Seq[(String, Expression)])(implicit expressionEvaluator: ExpressionEvaluator, ctx: ExpressionContext): DataFrame = {
    val schema = columns.map(col =>
      col._1 -> typeOf(col._2, Map.empty)
    )

    DataFrame(schema, () => Iterator.single(
      columns.map(col => {
        expressionEvaluator.eval(col._2)(ctx)
      })))
  }
}

trait DataFrameOperator {
  def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame

  def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame

  def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame

  def skip(df: DataFrame, num: Int): DataFrame

  def take(df: DataFrame, num: Int): DataFrame

  def join(a: DataFrame, b: DataFrame): DataFrame

  def distinct(df: DataFrame): DataFrame
}

class DataFrameOperatorImpl(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {
  def distinct(df: DataFrame): DataFrame = DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = {
    val schema1: Map[String, (CypherType, Int)] = df.schema.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    val schema2 = columns.map { column =>
      column._2.getOrElse(column._1) -> schema1(column._1)._1
    }

    DataFrame(
      schema2,
      () => df.records.map {
        row =>
          columns.map(column => row.apply(schema1(column._1)._2))
      }
    )
  }

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val schema1 = df.schema

    val schema2 = columns.map(col =>
      col._1 -> typeOf(col._2, schema1.toMap)
    )

    DataFrame(schema2,
      () => df.records.map(
        record => {
          columns.map(col => {
            expressionEvaluator.eval(col._2)(ctx.withVars(schema1.map(_._1).zip(record).toMap))
          })
        }
      )
    )
  }

  override def filter(df: DataFrame, predicate: (Seq[LynxValue]) => Boolean)(ctx: ExpressionContext): DataFrame = {
    DataFrame(df.schema,
      () => df.records.filter(predicate(_))
    )
  }

  override def skip(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.take(num))

  override def join(a: DataFrame, b: DataFrame): DataFrame = {
    val colsa = a.schema.map(_._1).zipWithIndex.toMap
    val colsb = b.schema.map(_._1).zipWithIndex.toMap
    //["m", "n"]
    val joinCols = a.schema.map(_._1).filter(colsb.contains(_))
    val (smallTable, largeTable, smallColumns, largeColumns) =
      if (a.records.size < b.records.size) {
        (a, b, colsa, colsb)
      }
      else {
        (b, a, colsb, colsa)
      }

    //{1->"m", 2->"n"}
    val largeColumns2 = (largeColumns -- joinCols).map(_.swap)
    val joinedSchema = smallTable.schema ++ (largeTable.schema.filter(x => !joinCols.contains(x._1)))

    DataFrame(joinedSchema, () => {
      val smallMap: Map[Seq[LynxValue], Iterable[(Seq[LynxValue], Seq[LynxValue])]] =
        smallTable.records.map {
          row => {
            val value = joinCols.map(joinCol => row(smallColumns(joinCol)))
            value -> row
          }
        }.toIterable.groupBy(_._1)

      val joinedRecords = largeTable.records.flatMap {
        row => {
          val value: Seq[LynxValue] = joinCols.map(joinCol => row(largeColumns(joinCol)))
          smallMap.getOrElse(value, Seq()).map(x => x._2 ++ largeColumns2.map(x => row(x._1)))
        }
      }

      joinedRecords.filter(
        item => {
          //(m)-[r]-(n)-[p]-(t), r!=p
          val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
          relIds.size == relIds.toSet.size
        }
      )
    })
  }
}

trait DataFrameOps {
  val srcFrame: DataFrame
  val operator: DataFrameOperator

  def select(columns: Seq[(String, Option[String])]): DataFrame = operator.select(srcFrame, columns)

  def project(columns: Seq[(String, Expression)])(implicit ctx: ExpressionContext): DataFrame = operator.project(srcFrame, columns)(ctx)

  def join(b: DataFrame): DataFrame = operator.join(srcFrame, b)

  def filter(predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame = operator.filter(srcFrame, predicate)(ctx)

  def take(num: Int): DataFrame = operator.take(srcFrame, num)

  def skip(num: Int): DataFrame = operator.skip(srcFrame, num)

  def distinct(): DataFrame = operator.distinct(srcFrame)
}

object DataFrameOps {
  implicit def ops(ds: DataFrame)(implicit dfo: DataFrameOperator): DataFrameOps = DataFrameOps(ds)(dfo)

  def apply(ds: DataFrame)(dfo: DataFrameOperator): DataFrameOps = new DataFrameOps {
    override val srcFrame: DataFrame = ds
    val operator: DataFrameOperator = dfo
  }
}