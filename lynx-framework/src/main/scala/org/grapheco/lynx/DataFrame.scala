package org.grapheco.lynx

import org.opencypher.v9_0.ast.SortItem
import org.opencypher.v9_0.expressions.{BooleanLiteral, DoubleLiteral, Expression, IntegerLiteral, Parameter, Property, StringLiteral, True, Variable}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait DataFrame {
  def schema: Seq[(String, LynxType)]

  def records: Iterator[Seq[LynxValue]]
}


object DataFrame {
  def empty: DataFrame = DataFrame(Seq.empty, () => Iterator.empty)

  def apply(schema0: Seq[(String, LynxType)], records0: () => Iterator[Seq[LynxValue]]) =
    new DataFrame {
      override def schema = schema0

      override def records = records0()
    }

  def cached(schema0: Seq[(String, LynxType)], records: Seq[Seq[LynxValue]]) =
    apply(schema0, () => records.iterator)

  def unit(columns: Seq[(String, Expression)])(implicit expressionEvaluator: ExpressionEvaluator, ctx: ExpressionContext): DataFrame = {
    val schema = columns.map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, Map.empty)
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

  def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame
}

class DefaultDataFrameOperator(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {
  def distinct(df: DataFrame): DataFrame = DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  private def sortByItem(a:Seq[LynxValue], b:Seq[LynxValue])(implicit sitem: Seq[(Expression, Boolean)], schema1: Map[String, (CypherType, Int)], ctx: ExpressionContext): Boolean = {
    val sd =sitem.foldLeft((true, true)){
      (f, s) => {
        f match {
          case (true, true) => {

            //expressionEvaluator.eval(s._1)
            //(ctx.withVars(schema1.map(_._1).zip(record).toMap))
            val ev1 = expressionEvaluator.eval(s._1)(ctx.withVars(schema1.map(_._1).zip(a).toMap))
            val ev2 = expressionEvaluator.eval(s._1)(ctx.withVars(schema1.map(_._1).zip(b).toMap))
            s._2 match{
              case true => (ev1 <= ev2, ev1==ev2)
              case false => (ev1 >= ev2, ev1==ev2)
            }
          }
          case (true, false) => (true, false)
          case (false, true) => (false, true)
          case (false, false) => (false, false)
        }
      }
    }
    sd._1
  }

  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = {
    val schema1: Map[String, (CypherType, Int)] = df.schema.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    DataFrame(df.schema, () => df.records.toSeq.sortWith(sortByItem(_,_)(sortItem, schema1, ctx)).toIterator)

  }


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

  private def groupBy(df: DataFrame): DataFrame ={

    val flagOfAggregation: Boolean = df.schema.exists(x => x._2.isInstanceOf[AggregationType])
    val flagOfCountStar: Boolean = df.schema.exists(x => x._2.isInstanceOf[CountStarType])

    var gBy:  mutable.IndexedSeq[(Int, CypherType)] = mutable.IndexedSeq[(Int, CypherType)]()
    var aggre: mutable.IndexedSeq[(Int, CypherType)] = mutable.IndexedSeq[(Int, CypherType)]()


    if (flagOfAggregation){
      if (flagOfCountStar) DataFrame(df.schema, () => Iterator(Seq(LynxInteger(df.records.size))))
      else {
        df.schema.foreach(s => {
          s._2 match {
            case avgType: AvgType => aggre = aggre ++ Seq(df.schema.indexOf(s) ->s._2)
            case maxType: MaxType => aggre = aggre ++ Seq(df.schema.indexOf(s) ->s._2)
            case minType: MinType => aggre = aggre ++ Seq(df.schema.indexOf(s) ->s._2)
            case sumType: SumType => aggre = aggre ++ Seq(df.schema.indexOf(s) ->s._2)
            case countType: CountType => aggre = aggre ++ Seq(df.schema.indexOf(s) ->s._2)
            case _ => gBy = gBy ++ Seq(df.schema.indexOf(s) ->s._2)
          }
        })

        def single(a: Seq[LynxValue], aggre: mutable.IndexedSeq[(Int, CypherType)]): Seq[LynxValue] ={
          a.map(s => {
            val idex = a.indexOf(s)
            aggre.toMap.get(idex) match {
              case None => LynxNull
              case Some(value) => value match {
                case avgType: AvgType => s
                case maxType: MaxType => s
                case minType: MinType => s
                case sumType: SumType => s
                case countType: CountType => LynxInteger(1)
              }
            }
          }).filter(!_.equals(LynxNull))
        }
        def zero(): Seq[LynxValue] ={
          df.schema.map(s => {
            s._2 match {
              case avgType: AvgType => LynxNull
              case maxType: MaxType => LynxNull
              case minType: MinType => LynxNull
              case sumType: SumType => LynxNull
              case countType: CountType => LynxInteger(0)
              case _ => LynxNull
            }
          })
        }


        def combine(a: Seq[LynxValue], b: Seq[LynxValue], aggre: mutable.IndexedSeq[(Int, CypherType)], size: Int): Seq[LynxValue] = {
          a.map(s => {
              val idex = a.indexOf(s)
              aggre.toMap.get(idex) match {
                case None => LynxNull
                case Some(value) => value match {
                  case avgType: AvgType => LynxDouble(s.value.asInstanceOf[Float] / size + b(idex).value.asInstanceOf[Float] / size)
                  case maxType: MaxType => if (s >= b(idex)) s else b(idex)
                  case minType: MinType => if (s <= b(idex)) s else b(idex)
                  case sumType: SumType => s.asInstanceOf[LynxNumber] + b(idex).asInstanceOf[LynxNumber]
                  case countType: CountType => s.asInstanceOf[LynxNumber] + b(idex).asInstanceOf[LynxNumber]//LynxInteger(size)
                }
              }
            }).filter(!_.equals(LynxNull))

        }
        if(gBy.isEmpty) DataFrame(df.schema, ()=> Iterator(
          df.records.size match {
            case 1 => single(df.records.toList.head, aggre)
            case 0 => zero()
            case _ =>
              val dfs = df.records.size
              df.records.toList.reduce((a,b) => combine(a,b, aggre, dfs))
          }
        ))

        else{
          DataFrame(df.schema,
            () =>df.records.toList.groupBy(r => gBy.map(_._1).toSeq.map(r(_))).mapValues(l =>
                  l.size match {
                    case 1 => single(l.head, aggre)
                    case 0 => zero()
                    case _ => l.reduce((a, b) => combine(a, b, aggre, l.size))
                  }

            ).map(x => x._1 ++ x._2).toIterator)
        }


      }
    }
    else df

  }
  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val schema1 = df.schema



    val schema2 = columns.map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, schema1.toMap)
    )

    val dfn:DataFrame = DataFrame(schema2,
      () => df.records.map(
        record => {
          columns.map(col => {
            expressionEvaluator.eval(col._2)(ctx.withVars(schema1.map(_._1).zip(record).toMap))
          })
        }
      )
    )
    groupBy(dfn)
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


  def orderBy(sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = operator.orderBy(srcFrame, sortItem)(ctx)
}

object DataFrameOps {
  implicit def ops(ds: DataFrame)(implicit dfo: DataFrameOperator): DataFrameOps = DataFrameOps(ds)(dfo)

  def apply(ds: DataFrame)(dfo: DataFrameOperator): DataFrameOps = new DataFrameOps {
    override val srcFrame: DataFrame = ds
    val operator: DataFrameOperator = dfo
  }
}