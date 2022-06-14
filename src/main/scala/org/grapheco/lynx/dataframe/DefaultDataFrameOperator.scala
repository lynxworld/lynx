package org.grapheco.lynx.dataframe

import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.LynxRelationship
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.CypherType

/**
 * @ClassName DefaultDataFrameOperator
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultDataFrameOperator(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {
  def distinct(df: DataFrame): DataFrame = DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  private def sortByItem(a: Seq[LynxValue],
                         b: Seq[LynxValue],
                         items: Seq[(Expression, Boolean)],
                         schema: Map[String, (CypherType, Int)],
                         ctx: ExpressionContext): Boolean = {
    val sd = items.foldLeft((true, true)) {
      (f, s) => {
        f match {
          case (true, true) => {

            //expressionEvaluator.eval(s._1)
            //(ctx.withVars(schema1.map(_._1).zip(record).toMap))
            val ev1 = expressionEvaluator.eval(s._1)(ctx.withVars(schema.map(_._1).zip(a).toMap))
            val ev2 = expressionEvaluator.eval(s._1)(ctx.withVars(schema.map(_._1).zip(b).toMap))
            s._2 match {
              // LynxNull = MAX
              case true => {
                if (ev1 == LynxNull && ev2 != LynxNull) (false, false)
                else if (ev1 == LynxNull && ev2 == LynxNull) (true, true)
                else if (ev1 != LynxNull && ev2 == LynxNull) (true, false)
                else (ev1 <= ev2, ev1 == ev2)
              }
              case false => {
                if (ev1 == LynxNull && ev2 != LynxNull) (true, false)
                else if (ev1 == LynxNull && ev2 == LynxNull) (true, true)
                else if (ev1 != LynxNull && ev2 == LynxNull) (false, false)
                else (ev1 >= ev2, ev1 == ev2)
              }
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
    DataFrame(df.schema, () => df.records.toSeq.sortWith(sortByItem(_, _, sortItem, schema1, ctx)).toIterator)
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

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val schema1 = df.schema
    val schema2 = columns.map {
      case (name, expression) => name -> expressionEvaluator.typeOf(expression, schema1.toMap)
    }

    val colNames = schema1.map { case (name, lynxType) => name }

    DataFrame(schema2,
      () => df.records.map(
        record => {
          val recordCtx = ctx.withVars(colNames.zip(record).toMap)
          columns.map(col => expressionEvaluator.eval(col._2)(recordCtx)) //TODO: to opt
        }
      )
    )
  }

  // TODO rewrite it.
  override def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {

    // match (n:nothislabel) return count(n)
    val schema1 = df.schema
    val schema2 = (groupings ++ aggregations).map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, schema1.toMap)
    )
    val columnsName = df.columnsName

    DataFrame(schema2, () => {
      if (groupings.nonEmpty) {
        df.records.map { record =>
          val recordCtx = ctx.withVars(columnsName.zip(record).toMap)
          groupings.map(col => expressionEvaluator.eval(col._2)(recordCtx)) -> recordCtx
        } // (groupingValue: Seq[LynxValue] -> recordCtx: ExpressionContext)
          .toSeq.groupBy(_._1) // #group by 'groupingValue'.
          .mapValues(_.map(_._2)) // #trans to: (groupingValue: Seq[LynxValue] -> recordsCtx: Seq[ExpressionContext])
          .map { case (groupingValue, recordsCtx) => // #aggragate: (groupingValues & aggregationValues): Seq[LynxValue]
            groupingValue ++ {
              aggregations.map { case (name, expr) => expressionEvaluator.aggregateEval(expr)(recordsCtx) }
            }
          }.toIterator
      } else {
        val allRecordsCtx = df.records.map { record => ctx.withVars(columnsName.zip(record).toMap) }.toSeq
        Iterator(aggregations.map { case (name, expr) => expressionEvaluator.aggregateEval(expr)(allRecordsCtx) })
      }
    })
  }

  override def filter(df: DataFrame, predicate: (Seq[LynxValue]) => Boolean)(ctx: ExpressionContext): DataFrame = {
    DataFrame(df.schema,
      () => df.records.filter(predicate)
    )
  }

  override def skip(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.take(num))

  override def join(a: DataFrame, b: DataFrame, isSinglesMatch: Boolean, bigTableIndex: Int): DataFrame = {
    val colsa = a.schema.map(_._1).zipWithIndex.toMap
    val colsb = b.schema.map(_._1).zipWithIndex.toMap
    //["m", "n"]
    val joinCols = a.schema.map(_._1).filter(colsb.contains(_))
    val (smallTable, largeTable, smallColumns, largeColumns, swapped) =
      if (bigTableIndex == 1) {
        (a, b, colsa, colsb, false)
      }
      else {
        (b, a, colsb, colsa, true)
      }

    //{1->"m", 2->"n"}
    val largeColumns2 = (largeColumns -- joinCols).map(_.swap)
    val joinedSchema = a.schema ++ b.schema.filter(x => !joinCols.contains(x._1))
    val needCheckSkip = isSinglesMatch && a.schema.size == b.schema.size

    DataFrame(joinedSchema, () => {
      val smallMap: Map[Seq[LynxValue], Iterable[(Seq[LynxValue], Seq[LynxValue])]] = {
        smallTable.records.map {
          row => {
            val value = joinCols.map(joinCol => row(smallColumns(joinCol)))
            value -> row
          }
        }.toIterable.groupBy(_._1)
      }

      val joinedRecords = largeTable.records.flatMap {
        row => {
          val value = joinCols.map(joinCol => row(largeColumns(joinCol)))
          smallMap.getOrElse(value, Seq()).map(x => {
            val lvs = largeColumns2.map(lc => row(lc._1)).toSeq
            if (swapped) {
              lvs ++ x._2
            } else {
              x._2 ++ lvs
            }
          })
        }
      }
      val result = if (needCheckSkip) {
        val gap = a.schema.size
        joinedRecords.filterNot(r => {
          var identical: Boolean = true
          for (i <- 0 until gap) {
            if (r(i).value != r(i + gap).value) {
              identical = false
            }
          }
          identical
        })
      } else {
        joinedRecords
      }
      result.filter(
        item => {
          //(m)-[r]-(n)-[p]-(t), r!=p
          val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
          relIds.size == relIds.toSet.size
        }
      )
    })
  }
}
