package org.opencypher.lynx.plan

import org.opencypher.lynx.{EvalContext, LynxTable, RecordHeader, TableOperator}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBigDecimal, CypherBoolean, CypherFloat, CypherInteger, CypherMap, CypherNull, CypherValue, Element}
import org.opencypher.okapi.impl.util.{PrintOptions, TablePrinter}
import org.opencypher.okapi.ir.api.expr.{Aggregator, AliasExpr, CountStar, ElementProperty, EndNode, Equals, Expr, FalseLit, GreaterThan, HasLabel, IsNotNull, IsNull, Not, NullLit, Param, StartNode, TrueLit, Type, Var}
import org.opencypher.v9_0.expressions.ASTBlobLiteral
import org.opencypher.okapi.api.value.CypherValue.Format._
import scala.collection.Seq

class SimpleTableOperator extends TableOperator {
  override def show(table: LynxTable, rows: Int = 20): Unit = {
    val columns = table.schema.map(_._1)
    implicit val options: PrintOptions = PrintOptions.out
    val content: Seq[Seq[CypherValue]] = table.records.take(rows).map { row =>
      columns.foldLeft(Seq.empty[CypherValue]) {
        case (currentSeq, column) => currentSeq :+ table.cell(row, column)
      }
    }.toSeq

    options.stream
      .append(TablePrinter.toTable(columns.toSeq, content)(v => v.toCypherString))
      .flush()
  }

  override def cache(table: LynxTable) = table

  override def aggregate(expr: Expr, values: Seq[CypherValue]): CypherValue = {
    expr match {
      case CountStar => values.size
    }
  }

  override def eval(expr: Expr)(implicit ctx: EvalContext): CypherValue = {
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

  override def select(table: LynxTable, cols: String*): LynxTable = {
    selectWithAlias(table, cols.zip(cols): _*)
  }

  override def selectWithAlias(table: LynxTable, cols: (String, String)*): LynxTable = {
    LynxTable(
      cols.map {
        column =>
          column._2 -> table.columnType(column._1)
      },
      table.records.map {
        row =>
          cols.map {
            column =>
              table.cell(row, column._1)
          }
      }
    )
  }

  override def filter(table: LynxTable, expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): LynxTable = {
    LynxTable(table.schema,
      table.records.filter { row =>
        implicit val ctx = EvalContext(header, (column) => table.cell(row, column), parameters)
        eval(expr) match {
          case CypherNull => false
          case CypherBoolean(x) => x
        }
      })
  }

  override def drop(table: LynxTable, cols: String*): LynxTable = {
    val remained = table.schema.dropWhile(col => cols.contains(col)).map(_._1)
    select(table, remained.toSeq: _*)
  }

  /*
   table a:
   ╔════════╗
   ║  node  ║
   ╠════════╣
   ║   1    ║
   ║   2    ║
   ╚════════╝
   table b:
   ╔═════╤════════╤════════╗
   ║ rel │ source │ target ║
   ╠═════╪════════╪════════╣
   ║  1  │    1   │   2    ║
   ║  2  │    2   │   3    ║
   ╚═════╧════════╧════════╝
   joinCols: a.node=b.source
   expected:
   ╔══════╤═════╤════════╤════════╗
   ║ node │ rel │ source │ target ║
   ╠══════╪═════╪════════╪════════╣
   ║  1   │  1  │    1   │   2    ║
   ║  2   │  2  │    2   │   3    ║
   ╚══════╧═════╧════════╧════════╝
  */

  //a.schema={"node","rel","source","target"}
  //b.schema={"n__NODE"}
  //joinCols={"target"->"n__NODE"}
  override def join(a: LynxTable, b: LynxTable, joinType: JoinType, joinCols: (String, String)*): LynxTable = {
    joinType match {
      case InnerJoin =>
        val (smallTable, largeTable, smallColumns, largeColumns) =
          if (a.size < b.size) {
            (a, b, joinCols.map(_._1), joinCols.map(_._2))
          }
          else {
            (b, a, joinCols.map(_._2), joinCols.map(_._1))
          }

        val smallMap: Map[Seq[CypherValue], Iterable[(Seq[CypherValue], Seq[CypherValue])]] =
          smallTable.records.map {
            row => {
              //joinCols: a.node=b.source
              val value = smallColumns.map(joinCol => smallTable.cell(row, joinCol))
              value -> row
            }
          }.groupBy(_._1)

        val joinedSchema = smallTable.schema ++ largeTable.schema
        val joinedRecords = largeTable.records.flatMap {
          row => {
            val value = largeColumns.map(joinCol => largeTable.cell(row, joinCol))
            smallMap.getOrElse(value, Seq()).map(x => x._2 ++ row)
          }
        }

        LynxTable(joinedSchema, joinedRecords)
    }
  }

  override def unionAll(a: LynxTable, b: LynxTable): LynxTable = {
    //TODO: large number of records
    LynxTable((a.schema ++ b.schema).distinct, a.records.toSeq.union(b.records.toSeq))
  }

  override def orderBy(table: LynxTable, sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): LynxTable = {
    ???
  }

  override def skip(table: LynxTable, n: Long): LynxTable = {
    LynxTable(table.schema, table.records.drop(n.toInt))
  }

  override def limit(table: LynxTable, n: Long): LynxTable = {
    LynxTable(table.schema, table.records.take(n.toInt))
  }

  override def distinct(table: LynxTable, cols: String*): LynxTable = {
    val table2 = select(table, cols: _*)
    LynxTable(table2.schema, table2.records.toSeq.distinct)
  }

  override def group(table: LynxTable, by: Set[Var], aggregations: Map[String, Aggregator])
                    (implicit header: RecordHeader, parameters: CypherMap): LynxTable = {
    //by=[Var(`class`), Var(`sex`)]
    //aggregations={`avg_age`->Avg(`age`), `max_age`->Max(`age`)}

    //df1=
    // ((`class`=4, `sex`=0) -> Stream[{...}, {...}])
    // ((`class`=4, `sex`=1) -> Stream[{...}, {...}])
    // ((`class`=5, `sex`=0) -> Stream[{...}, {...}])
    //TODO: large number of records
    val df1 = table.records.toSeq.groupBy(row =>
      by.map(eval(_)(EvalContext(header, table.cell(row, _), parameters)))
    )

    //df2=
    //`class`=4, `sex`=0, `avg_age`=9, `max_age`=10
    //TODO: large number of records
    val df2 = df1.map(groupped =>
      groupped._1.toSeq ++
        aggregations.map(agr =>
          aggregate(agr._2, groupped._2.map(row => table.cell(row, agr._1)))
        ).toSeq
    )

    LynxTable(by.toSeq.map(v => v.name -> v.cypherType) ++ aggregations.map(agr => agr._1 -> agr._2.cypherType),
      df2)
  }

  override def withColumns(table: LynxTable, columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): LynxTable = {
    LynxTable(table.schema ++ columns.map(column => column._2 -> column._1.cypherType),
      table.records.map(row =>
        row ++ columns.map(column => {
          implicit val ctx = EvalContext(header, table.cell(row, _), parameters)
          eval(column._1)
        })
      )
    )
  }
}
