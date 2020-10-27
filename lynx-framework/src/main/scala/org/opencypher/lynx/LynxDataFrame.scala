package org.opencypher.lynx

import org.opencypher.lynx.planning.{InnerJoin, JoinType, Order}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Format._
import org.opencypher.okapi.api.value.CypherValue.{CypherBigDecimal, CypherBoolean, CypherFloat, CypherInteger, CypherMap, CypherNull, CypherValue, Element}
import org.opencypher.okapi.impl.util.{PrintOptions, TablePrinter}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions.ASTBlobLiteral

import scala.collection.Seq

//meta: (name,STRING),(age,INTEGER)
class LynxDataFrame(val schema: Set[(String, CypherType)], val records: Seq[Seq[_ <: CypherValue]])
                   (implicit session: LynxSession) extends DataFrame {
  val columnIndex = schema.zipWithIndex.map(x => x._1._1 -> x._2).toMap
  override val columnType: Map[String, CypherType] = schema.toMap

  override def cache() = this

  def aggregate(expr: Expr, values: Seq[CypherValue]): CypherValue = {
    expr match {
      case CountStar => values.size
    }
  }

  def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue = {
    val EvalContext(header, valueOfColumn, parameters) = ctx

    expr match {
      case IsNull(expr) =>
        eval(expr) match {
          case CypherNull => true
          case _ => false
        }

      case IsNotNull(expr) =>
        eval(expr) match {
          case CypherNull => false
          case _ => true
        }

      case Not(expr) =>
        eval(expr) match {
          case CypherNull => CypherNull
          case CypherBoolean(x) => CypherBoolean(!x)
        }

      case Equals(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (CypherNull, _) => CypherNull
          case (_, CypherNull) => CypherNull
          case (x, y) => x.equals(y)
        }

      case GreaterThan(lhs: Expr, rhs: Expr) =>
        (eval(lhs), eval(rhs)) match {
          case (CypherBigDecimal(v1), CypherBigDecimal(v2)) => v1.compareTo(v2) > 0
          case (CypherInteger(v1), CypherInteger(v2)) => v1.compareTo(v2) > 0
          case (CypherFloat(v1), CypherFloat(v2)) => v1.compareTo(v2) > 0
          case (CypherNull, _) => CypherNull
        }

      case param: Param =>
        parameters(param.name)

      case _: Var | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
        //record(expr.withoutType)
        valueOfColumn(header.column(expr))

      case ep: ElementProperty =>
        (ep.key.name -> eval(ep.propertyOwner)) match {
          //TODO: lazy load of Element.property()
          case (name, e: Element[_]) => e.properties(name)
        }

      case AliasExpr(expr0: Expr, alias: Var) =>
        eval(expr0)

      case TrueLit =>
        true

      case FalseLit =>
        false

      case NullLit =>
        CypherNull

      case lit: ASTBlobLiteral =>
        CypherValue(lit.value)
    }
  }

  private def cell(row: Seq[_ <: CypherValue], column: String): CypherValue =
    row(columnIndex(column))

  override def select(cols: String*): LynxDataFrame = {
    val tuples = cols.zip(cols)
    select(tuples.head, tuples.tail: _*)
  }

  override def select(col: (String, String), cols: (String, String)*): LynxDataFrame= {
    val columns = col +: cols

    new LynxDataFrame(columns.map(column =>
      column._2 -> columnType(column._1)).toSet,
      records.map(row =>
        columns.map(column => cell(row, column._1)))
    )
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame= {
    new LynxDataFrame(schema,
      records.filter { row =>
        implicit val ctx = EvalContext(header, (column) => cell(row, column), parameters)
        eval(expr) match {
          case CypherNull => false
          case CypherBoolean(x) => x
        }
      })
  }

  override def drop(cols: String*): LynxDataFrame= {
    val remained = schema.dropWhile(col => cols.contains(col)).map(_._1)
    select(remained)
  }

  override def join(df: DataFrame, joinType: JoinType, joinCols: (String, String)*): LynxDataFrame= {
    val other = df.asInstanceOf[LynxDataFrame]
    joinType match {
      case InnerJoin => {
        val joined = this.records.flatMap {
          thisRow => {
            other.records.filter {
              otherRow => {
                joinCols.map(joinCol =>
                  this.cell(thisRow, joinCol._1) ==
                    other.cell(otherRow, joinCol._2)).reduce(_ && _)
              }
            }.map {
              thisRow ++ _
            }
          }
        }

        new LynxDataFrame(this.schema ++ other.schema, joined)
      }
    }
  }

  override def unionAll(df: DataFrame): LynxDataFrame= {
    val other = df.asInstanceOf[LynxDataFrame]
    new LynxDataFrame(this.schema ++ other.schema, this.records.union(other.records))
  }

  override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame= {
    ???
  }

  def skip(n: Long): LynxDataFrame= {
    new LynxDataFrame(this.schema, this.records.drop(n.toInt))
  }

  override def limit(n: Long): LynxDataFrame= {
    new LynxDataFrame(this.schema, this.records.take(n.toInt))
  }

  override def distinct: LynxDataFrame= {
    new LynxDataFrame(this.schema, this.records.distinct)
  }

  override def distinct(cols: String*): LynxDataFrame= {
    this.select(cols: _*).distinct
  }

  override def group(by: Set[Var], aggregations: Map[String, Aggregator])
                    (implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame= {
    //by=[Var(`class`), Var(`sex`)]
    //aggregations={`avg_age`->Avg(`age`), `max_age`->Max(`age`)}

    //df1=
    // ((`class`=4, `sex`=0) -> Stream[{...}, {...}])
    // ((`class`=4, `sex`=1) -> Stream[{...}, {...}])
    // ((`class`=5, `sex`=0) -> Stream[{...}, {...}])
    val df1 = records.groupBy(row =>
      by.map(eval(_)(EvalContext(header, cell(row, _), parameters)))
    )

    //df2=
    //`class`=4, `sex`=0, `avg_age`=9, `max_age`=10

    val df2 = df1.map(groupped =>
      groupped._1.toSeq ++
        aggregations.map(agr =>
          aggregate(agr._2, groupped._2.map(row => cell(row, agr._1)))
        ).toSeq
    )

    new LynxDataFrame(by.map(v => v.name -> v.cypherType) ++ aggregations.map(agr => agr._1 -> agr._2.cypherType),
      df2.toSeq)
  }

  override def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): LynxDataFrame= {
    new LynxDataFrame(schema ++ columns.map(column => column._2 -> column._1.cypherType),
      records.map(row =>
        row ++ columns.map(column => {
          implicit val ctx = EvalContext(header, cell(row, _), parameters)
          eval(column._1)
        })
      )
    )
  }

  override def show(rows: Int = 20): Unit = {
    val columns = schema.map(_._1)
    implicit val options: PrintOptions = PrintOptions.out
    val content: Seq[Seq[CypherValue]] = records.take(rows).map { row =>
      columns.foldLeft(Seq.empty[CypherValue]) {
        case (currentSeq, column) => currentSeq :+ cell(row, column)
      }
    }

    options.stream
      .append(TablePrinter.toTable(columns.toSeq, content)(v => v.toCypherString))
      .flush()
  }

  override def physicalColumns: Seq[String] = schema.map(_._1).toSeq

  override def rows: Iterator[String => CypherValue.CypherValue] =
    records.map(row => (key: String) => cell(row, key)).iterator

  override def size: Long = records.size
}

case class EvalContext(header: RecordHeader, valueOfColumn: (String) => CypherValue, parameters: CypherMap) {

}
