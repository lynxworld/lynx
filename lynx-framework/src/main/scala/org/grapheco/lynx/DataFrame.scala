package org.grapheco.lynx

import org.grapheco.lynx.DataFrame.typeOf
import org.opencypher.v9_0.expressions.{BooleanLiteral, DoubleLiteral, Expression, IntegerLiteral, Parameter, Property, StringLiteral, True, Variable}
import org.opencypher.v9_0.util.symbols.{CTBoolean, CTFloat, CTInteger, CTString, CypherType}

trait DataFrame {
  def schema: Seq[(String, CypherType)]

  def records: Iterator[Seq[LynxValue]]
}

object DataFrame {
  def empty: DataFrame = DataFrame(Seq.empty, () => Iterator.empty)

  def typeOf(expr: Expression, varTypes: Map[String, CypherType]): CypherType =
    expr match {
      case Parameter(name, parameterType) => parameterType

      //literal
      case _: BooleanLiteral => CTBoolean
      case _: StringLiteral => CTString
      case _: IntegerLiteral => CTInteger
      case _: DoubleLiteral => CTFloat

      case Variable(name) => varTypes(name)
      case p@Property(_, _) => CTInteger
    }

  def apply(schema0: Seq[(String, CypherType)], records0: () => Iterator[Seq[LynxValue]]) =
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

  def filter(df: DataFrame, condition: Expression)(ctx: ExpressionContext): DataFrame

  def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame

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

  override def filter(df: DataFrame, condition: Expression)(ctx: ExpressionContext): DataFrame = {
    val schema1 = df.schema

    DataFrame(schema1,
      () => df.records.filter(
        record =>
          expressionEvaluator.eval(condition)(ctx.withVars(schema1.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull => false
          }
      ))
  }
}

trait DataFrameOps {
  val srcFrame: DataFrame
  val operator: DataFrameOperator

  def select(columns: Seq[(String, Option[String])]): DataFrame = operator.select(srcFrame, columns)

  def project(columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = operator.project(srcFrame, columns)(ctx)

  def filter(condition: Option[Expression])(ctx: ExpressionContext): DataFrame =
    condition.map(operator.filter(srcFrame, _)(ctx)).getOrElse(srcFrame)

  def filter(condition: Expression)(ctx: ExpressionContext): DataFrame = operator.filter(srcFrame, condition)(ctx)

  def distinct(): DataFrame = operator.distinct(srcFrame)
}

object DataFrameOps {
  implicit def ops(ds: DataFrame)(implicit dfo: DataFrameOperator): DataFrameOps = DataFrameOps(ds)(dfo)

  def apply(ds: DataFrame)(dfo: DataFrameOperator): DataFrameOps = new DataFrameOps {
    override val srcFrame: DataFrame = ds
    val operator: DataFrameOperator = dfo
  }
}