package cn.pandadb.lynx

import org.opencypher.okapi.api.types.{CTNode, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap}
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

trait CypherRecordsLike {
  def table: LynxDataFrame

  def header: RecordHeader

  def cache(): this.type = ???

  private def wrapRow(row: CypherValue.CypherMap): CypherValue.CypherMap = {
    CypherMap(header.returnItems.map(item => {
      item.name -> wrapValue(item, row)
    }).toSeq: _*
    )
  }

  private def wrapValue(expr: Expr, row: CypherValue.CypherMap): Any = {
    expr.cypherType match {
      case CTNode(_, _) => {
        val id = row(header.column(expr)).asInstanceOf[CypherInteger].value
        LynxNode(id)
      }
      case _ => row(header.column(expr))
    }
  }

  private def wrapTable() = table.records.map(wrapRow(_))

  def iterator: Iterator[CypherValue.CypherMap] = wrapTable.iterator

  def collect: Array[CypherValue.CypherMap] = wrapTable.toArray

  def columnType: Map[String, CypherType] = header.exprToColumn.collect { case (v: Var, colname) => v.name -> v.cypherType }

  def rows: Iterator[String => CypherValue.CypherValue] = wrapTable.map(row => (col: String) => row(col)).iterator
}
