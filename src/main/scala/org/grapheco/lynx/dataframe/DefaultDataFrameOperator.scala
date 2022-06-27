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

  private def lessThan(sortValue: Iterator[(LynxValue, LynxValue, Boolean)]): Boolean = {
    while (sortValue.hasNext) {
      val (valueOfA, valueOfB, asc) = sortValue.next()
      val oA = LynxValue.typeOrder(valueOfA)
      val oB = LynxValue.typeOrder(valueOfB)
      //  AisBigger asc lessThan
      //  T T F
      //  T F T
      //  F T T
      //  F F F
      // lessThan = AisBigger xor asc
      if (oA == oB){ // same type
        val comparable = valueOfA.compareTo(valueOfB)
        if(comparable != 0) return comparable > 0 != asc
      } else { // diff type
        return (oA > oB) != asc
      }
    }
    false
  }

  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = {
    val columnsName = df.columnsName
    DataFrame(df.schema, () => df.records.toSeq
      .sortWith{ (A,B) =>
        val ctxA = ctx.withVars(columnsName.zip(A).toMap)
        val ctxB = ctx.withVars(columnsName.zip(B).toMap)
        val sortValue = sortItem.map{ case(exp, asc) =>
          (expressionEvaluator.eval(exp)(ctxA), expressionEvaluator.eval(exp)(ctxB), asc)
        }
        lessThan(sortValue.toIterator)
      }.toIterator)
  }

  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = {
    val df_schema = df.schema.toMap
    val df_indexes = df.columnsName.zipWithIndex.toMap
    val indexes = columns.map(_._1).map(df_indexes)
    val schema = columns.map{ column =>
      column._2.getOrElse(column._1) -> df_schema(column._1)
    }
    DataFrame(
      schema, () => df.records.map{
        row =>indexes.map(row.apply)
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

  override def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame =
    DataFrame(df.schema, () => df.records.filter(predicate))

  override def skip(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.take(num))

  override def join(a: DataFrame, b: DataFrame, joinColumn: Seq[String], joinType: JoinType): DataFrame = {
//
    val colsa = a.schema.map(_._1).zipWithIndex.toMap
    val colsb = b.schema.map(_._1).zipWithIndex.toMap
    //["m", "n"]
    val joinCols = a.schema.map(_._1).filter(colsb.contains)
    val (smallTable, largeTable, smallColumns, largeColumns, swapped) =
//      if (bigTableIndex == 1) {
//        (a, b, colsa, colsb, false)
//      }
//      else {
        (b, a, colsb, colsa, true)
//      }

    //{1->"m", 2->"n"}
    val largeColumns2 = (largeColumns -- joinCols).map(_.swap)
    val joinedSchema = a.schema ++ b.schema.filter(x => !joinCols.contains(x._1))
    val needCheckSkip = a.schema.size == b.schema.size

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
