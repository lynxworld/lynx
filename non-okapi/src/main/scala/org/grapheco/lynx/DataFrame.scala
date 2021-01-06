package org.grapheco.lynx

import org.opencypher.v9_0.util.symbols.CypherType

trait DataFrame {
  def schema: Seq[(String, CypherType)]

  def records: Iterator[Seq[_ <: CypherValue]]
}

object DataFrame {
  def empty: DataFrame = null

  def apply(schema0: Seq[(String, CypherType)], records0: Iterable[Seq[_ <: CypherValue]]) =
    new DataFrame {
      override def schema = schema0

      override def records = records0.iterator
    }
}

trait DataFrameOperator {
  def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame
}
