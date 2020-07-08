package cn.pandadb.lynx

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types.{CTIdentity, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory, RelationalElementTableFactory, Table}
import org.opencypher.okapi.relational.impl.planning.{InnerJoin, JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.collection.mutable

class LynxSession extends RelationalCypherSession[LynxDataFrame] {
  protected implicit val self: LynxSession = this

  override val records: LynxRecordsFactory = LynxRecordsFactory()

  override val graphs: LynxGraphFactory = LynxGraphFactory()

  override val elementTables: LynxElementTableFactory = LynxElementTableFactory()

  def createGraphInMemory(elements: LynxElement*): PropertyGraph = {
    LynxDataSourceImpl(graphs, elementTables, elements: _*).getGraph()
  }
}

case class LynxGraphFactory()(implicit val session: LynxSession) extends RelationalCypherGraphFactory[LynxDataFrame] {
}

case class LynxElementTableFactory()(implicit val session: LynxSession) extends RelationalElementTableFactory[LynxDataFrame] {
  override def elementTable(elementMapping: ElementMapping, table: LynxDataFrame): ElementTable[LynxDataFrame] =
    LynxElementTable(elementMapping, table)
}

case class LynxRecordsFactory()(implicit session: LynxSession) extends RelationalCypherRecordsFactory[LynxDataFrame] {
  override type Records = LynxRecords

  override def unit(): LynxRecords = LynxRecords(RecordHeader.empty, LynxDataFrame.unit)

  override def empty(initialHeader: RecordHeader): LynxRecords = LynxRecords(initialHeader, LynxDataFrame.empty)

  override def fromElementTable(elementTable: ElementTable[LynxDataFrame]): LynxRecords = ???

  //wrap a result table as a records
  override def from(header: RecordHeader, table: LynxDataFrame, returnItems: Option[Seq[String]]): LynxRecords = {
    LynxRecords(header, table, returnItems)
  }
}

case class LynxElementTable(val mapping: ElementMapping, val table: LynxDataFrame)
  extends ElementTable[LynxDataFrame] with CypherRecordsLike {
  override type Records = this.type
}

case class LynxRecords(
                        header: RecordHeader,
                        table: LynxDataFrame,
                        override val logicalColumns: Option[Seq[String]] = None
                      )(implicit session: LynxSession) extends RelationalCypherRecords[LynxDataFrame] with CypherRecordsLike {
  override type Records = this.type

}

object LynxDataFrame {
  def empty(): LynxDataFrame = {
    LynxDataFrame(Map.empty, Stream.empty)(null)
  }

  def unit(): LynxDataFrame = {
    LynxDataFrame(Map.empty, Stream(Map.empty))(null)
  }
}

trait LynxDataSource {
  def getNodeById(id: Long): Option[LynxNode]
def getRelationshipById(id:Long):Option[LynxRelationship]
  def getGraph(): PropertyGraph
}

case class LynxDataSourceImpl(graphs: LynxGraphFactory, elementTables: LynxElementTableFactory, elements: LynxElement*) extends LynxDataSource {
  implicit val self = this
  val nodes = elements.filter(_.isInstanceOf[LynxNode]).map(_.asInstanceOf[LynxNode])
  val rels = elements.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship])

  val tables = nodes.groupBy(_.labels.headOption.getOrElse("")).map(kv => {
    val (label, groupped) = kv
    val schema = mutable.Map[String, CypherType]()
    groupped.foreach(record => {
      schema ++= record.withIds.value.map(x => x._1 -> x._2.cypherType)
      schema += "_id" -> CTIdentity
    })

    elementTables.elementTable(
      NodeMappingBuilder.create(nodeIdKey = "_id", impliedLabels = if (label.isEmpty) {
        Set.empty
      } else {
        Set(label)
      }, propertyKeys = schema.map(_._1).filter(!_.startsWith("_")).toSet),
      LynxDataFrame(schema.toMap, groupped.map(_.withIds).toStream))
  }) ++ rels.groupBy(_.relType).map(kv => {
    val (relType, groupped) = kv
    val schema = mutable.Map[String, CypherType]()
    groupped.foreach(record => {
      schema ++= record.withIds.value.map(x => x._1 -> x._2.cypherType)
      schema ++= Map("_id" -> CTIdentity, "_from" -> CTIdentity, "_to" -> CTIdentity)
    })

    elementTables.elementTable(
      RelationshipMappingBuilder.create("_id", "_from", "_to", relType, schema.map(_._1).filter(!_.startsWith("_")).toSet),
      LynxDataFrame(schema.toMap, groupped.map(_.withIds).toStream))
  })

  override def getNodeById(id: Long): Option[LynxNode] = nodes.find(_.id == id)
  override def getRelationshipById(id:Long):Option[LynxRelationship]= rels.find(_.id == id)

  override def getGraph(): PropertyGraph = graphs.create(None, tables.toSeq: _*)
}

//meta: (name,STRING),(age,INTEGER)
case class LynxDataFrame(schema: Map[String, CypherType], records: Stream[Map[String, CypherValue]])(implicit val dataSource: LynxDataSource) extends Table[LynxDataFrame] {
  //cols:(name,node_name__ STRING),(age,node_age __INTEGER)
  override def select(col: (String, String), cols: (String, String)*): LynxDataFrame = {
    val columns = col +: cols
    LynxDataFrame(columns.map(column =>
      column._2 -> schema(column._1)).toMap,
      records.map(record =>
        columns.map(column => column._2 -> record(column._1)).toMap)
    )
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxDataFrame = {
    LynxDataFrame(schema,
      records.filter { map =>
        implicit val ctx = EvalContext(header, map, parameters)
        Runtime.eval(expr) match {
          case CypherNull => false
          case CypherBoolean(x) => x
        }
      })
  }

  override def drop(cols: String*): LynxDataFrame = LynxDataFrame(schema -- cols, records.map(_ -- cols))

  override def join(other: LynxDataFrame, joinType: JoinType, joinCols: (String, String)*): LynxDataFrame = {
    joinType match {
      case InnerJoin => {
        val joined = this.records.flatMap {
          a => {
            other.records.filter { b => {
              joinCols.map(kv => a(kv._1) == b(kv._2)).reduce(_ && _)
            }
            }.map {
              b =>
                a ++ b
            }
          }
        }

        LynxDataFrame(this.schema ++ other.schema, joined)
      }
    }
  }

  override def unionAll(other: LynxDataFrame): LynxDataFrame = {
    LynxDataFrame(this.schema ++ other.schema, this.records.union(other.records))
  }

  override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxDataFrame = ???

  override def skip(n: Long): LynxDataFrame = ???

  override def limit(n: Long): LynxDataFrame = ???

  override def distinct: LynxDataFrame = ???

  override def distinct(cols: String*): LynxDataFrame = ???

  override def group(by: Set[Var], aggregations: Map[String, Aggregator])(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxDataFrame = ???

  //columns:(AliasExpr,AUTOINT0__ INTEGER)
  override def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): LynxDataFrame = {
    LynxDataFrame(schema ++ columns.map(column => column._2 -> column._1.cypherType),
      records.map(record =>
        record ++ columns.map(column => {
          implicit val ctx = EvalContext(header, record, parameters)
          column._2 -> Runtime.eval(column._1)
        })
      )
    )
  }

  override def show(rows: Int): Unit = {
    val rowsShown = records.take(rows)

    FormatUtils.printTable(physicalColumns, rowsShown.map(row => {
      physicalColumns.map(col => row.apply(col).toString())
    }).toArray.toSeq)
  }

  override def physicalColumns: Seq[String] = schema.keys.toSeq

  override def columnType: Map[String, CypherType] = schema

  override def rows: Iterator[String => CypherValue.CypherValue] = records.map(record => (colname: String) => record(colname)).iterator

  override def size: Long = records.size
}