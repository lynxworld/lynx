package cn.pandadb.lynx

import cn.pandadb.FormatUtils
import org.opencypher.lynx.{Eval, LynxNode}
import org.opencypher.okapi.api.graph.{NodePattern, PatternElement, PropertyGraph, RelationshipPattern, SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types.{CTIdentity, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory, RelationalElementTableFactory, Table}
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

class LynxSession extends RelationalCypherSession[LynxTable] {
  protected implicit val self: LynxSession = this

  override val records: LynxRecordsFactory = LynxRecordsFactory()

  override val graphs: LynxGraphFactory = LynxGraphFactory()

  override val elementTables: LynxElementTableFactory = LynxElementTableFactory()

  def createGraphInMemory(elements: LynxElement*): PropertyGraph = {
    val nodes = elements.filter(_.isInstanceOf[LynxNode]).map(_.asInstanceOf[LynxNode])
    val rels = elements.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship])

    val tables = nodes.groupBy(_.labels.headOption.getOrElse("_default")).map(kv => {
      val (label, groupped) = kv
      val schema = groupped.flatMap(record => record.withIds.value.map(x => x._1 -> x._2.cypherType) ++ Seq("_id" -> CTIdentity))
      elementTables.elementTable(
        NodeMappingBuilder.create(nodeIdKey = "_id", impliedLabels = Set(label), propertyKeys = schema.map(_._1).filter(!_.startsWith("_")).toSet),
        LynxTable(schema, groupped.map(_.withIds).toStream))
    }) ++ rels.groupBy(_.relType).map(kv => {
      val (relType, groupped) = kv
      val schema = groupped.flatMap(record => record.withIds.value.map(x => x._1 -> x._2.cypherType) ++ Seq("_id" -> CTIdentity, "_from" -> CTIdentity, "_to" -> CTIdentity))
      elementTables.elementTable(
        RelationshipMappingBuilder.create("_id", "_from", "_to", relType, schema.map(_._1).filter(!_.startsWith("_")).toSet),
        LynxTable(schema, groupped.map(_.withIds).toStream))
    })

    graphs.create(None, tables.toSeq: _*)
  }
}

object LynxNode {
  def apply(id: Long, props: (String, Any)*) = new LynxNode(id, Set.empty, props: _*)
}

case class LynxNode(id: Long, labels: Set[String], props: (String, Any)*) extends LynxElement {
  val properties = CypherMap(props: _*)
  val withIds = CypherMap(props ++ Seq("_id" -> id): _*)
}

case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, Any)*) extends LynxElement {
  val properties = CypherMap(props: _*)
  val withIds = CypherMap((props ++ Seq("_id" -> id, "_from" -> startId, "_to" -> endId)): _*)
}

trait LynxElement {

}

case class LynxGraphFactory()(implicit val session: LynxSession) extends RelationalCypherGraphFactory[LynxTable] {
}

case class LynxElementTableFactory()(implicit val session: LynxSession) extends RelationalElementTableFactory[LynxTable] {
  override def elementTable(elementMapping: ElementMapping, table: LynxTable): ElementTable[LynxTable] =
    LynxElementTable(elementMapping, table)
}

case class LynxRecordsFactory()(implicit morpheus: LynxSession) extends RelationalCypherRecordsFactory[LynxTable] {
  override type Records = LynxRecords

  override def unit(): LynxRecords = LynxRecords(RecordHeader.empty, LynxTable.empty)

  override def empty(initialHeader: RecordHeader): LynxRecords = LynxRecords(initialHeader, LynxTable.empty)

  override def fromElementTable(elementTable: ElementTable[LynxTable]): LynxRecords = ???

  override def from(header: RecordHeader, table: LynxTable, returnItems: Option[Seq[String]]): LynxRecords = {
    LynxRecords(header, table, returnItems)
  }
}

case class LynxElementTable(val mapping: ElementMapping, val table: LynxTable)
  extends ElementTable[LynxTable] with LynxElementTableLike {
  override type Records = this.type
}

trait LynxElementTableLike {
  def table: LynxTable

  def cache(): this.type = ???

  def iterator: Iterator[CypherValue.CypherMap] = table.records.iterator

  def collect: Array[CypherValue.CypherMap] = table.records.toArray

  def columnType: Map[String, CypherType] = table.columnType

  def rows: Iterator[String => CypherValue.CypherValue] = table.rows
}

case class LynxRecords(
                        header: RecordHeader,
                        table: LynxTable,
                        override val logicalColumns: Option[Seq[String]] = None
                      )(implicit session: LynxSession) extends RelationalCypherRecords[LynxTable] with LynxElementTableLike {
  override type Records = this.type
}

object LynxTable {
  def empty(): LynxTable = {
    LynxTable(Seq.empty, Stream.empty)
  }
}

case class LynxTable(meta: Seq[(String, CypherType)], records: Stream[CypherMap]) extends Table[LynxTable] {
  val columnMap = meta.toMap

  override def select(col: (String, String), cols: (String, String)*): LynxTable = {
    val columns = col +: cols
    LynxTable(columns.map(column => column._2 -> columnMap(column._1)),
      records.map(record => CypherMap(columns.map(column => column._2 -> record(column._1)): _*))
    )
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxTable = ???

  override def drop(cols: String*): LynxTable = ???

  override def join(other: LynxTable, joinType: JoinType, joinCols: (String, String)*): LynxTable = ???

  override def unionAll(other: LynxTable): LynxTable = ???

  override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxTable = ???

  override def skip(n: Long): LynxTable = ???

  override def limit(n: Long): LynxTable = ???

  override def distinct: LynxTable = ???

  override def distinct(cols: String*): LynxTable = ???

  override def group(by: Set[Var], aggregations: Map[String, Aggregator])(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxTable = ???

  override def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxTable = {
    LynxTable(meta ++ columns.map(column => column._2 -> column._1.cypherType),
      records.map(record => record ++ CypherMap(columns.map(column => column._2 -> Eval.eval(column._1, record, parameters)): _*)))
  }

  override def show(rows: Int): Unit = {
    val rowsShown = records.take(rows)

    FormatUtils.printTable(physicalColumns, rowsShown.map(row => {
      physicalColumns.map(col => row.apply(col).toString())
    }).toArray.toSeq)
  }

  override def physicalColumns: Seq[String] = meta.map(_._1)

  override def columnType: Map[String, CypherType] = meta.toMap

  override def rows: Iterator[String => CypherValue.CypherValue] = records.map(record => (colname: String) => record(colname)).iterator

  override def size: Long = ???
}