package org.grapheco.lynx

import org.opencypher.v9_0.util.symbols.CypherType

trait DataFrame {
  def schema: Seq[(String, CypherType)]

  def records: Iterator[Seq[CypherValue]]
}

object DataFrame {
  def empty: DataFrame = null

  def apply(schema0: Seq[(String, CypherType)], records0: () => Iterator[Seq[CypherValue]]) =
    new DataFrame {
      override def schema = schema0

      override def records = records0()
    }
}

trait DataFrameOperator {
  def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame
}

class DataFrameOperatorImpl extends DataFrameOperator {
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
}