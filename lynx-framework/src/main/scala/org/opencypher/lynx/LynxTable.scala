package org.opencypher.lynx

import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue

import scala.collection.Seq

object LynxTable {
  def unit: LynxTable = apply(Seq.empty[(String, CypherType)], Some(Seq[CypherValue]()))

  def apply(schema: Seq[(String, CypherType)], records: Iterable[Seq[_ <: CypherValue]]): LynxTable =
    new LynxTable(schema, records)

  def empty(schema: Seq[(String, CypherType)] = Seq.empty[(String, CypherType)]): LynxTable =
    apply(schema, None)
}

//meta: (name,STRING),(age,INTEGER)
class LynxTable(val schema: Seq[(String, CypherType)], val records: Iterable[Seq[_ <: CypherValue]]) extends CypherTable {
  private lazy val _columnIndex = schema.distinct.zipWithIndex.map(x => x._1._1 -> x._2).toMap
  override val columnType: Map[String, CypherType] = schema.toMap

  def cell(row: Seq[_ <: CypherValue], column: String): CypherValue =
    row(_columnIndex(column))

  override def physicalColumns: Seq[String] = schema.map(_._1).toSeq

  override def rows: Iterator[String => CypherValue.CypherValue] =
    records.iterator.map(row => (key: String) => cell(row, key))

  override def size: Long = records.size
}