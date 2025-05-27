package org.grapheco.lynx.dataframe

import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.{LTAny, LazyLynxValue, LynxType, LynxValue}
import org.grapheco.lynx.util.{ParallelismConfig, Profiler}
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:25 2022/7/5
 * @Modified By:
 */
class DefaultDataFrameOperator(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {

  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = {
    val sourceSchema: Map[String, LynxType] = df.schema.toMap
    val columnNameIndex: Map[String, Int] = df.columnsName.zipWithIndex.toMap
    val newSchema: Seq[(String, LynxType)] = columns.map(column => column._2.getOrElse(column._1) -> sourceSchema(column._1))
    val usedIndex: Seq[Int] = columns.map(_._1).map(columnNameIndex)

    DataFrame(newSchema, () => df.records.map(row => usedIndex.map(row.apply)))
  }

  override def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame =
    DataFrame(df.schema, () => df.records.grouped(ParallelismConfig.parallelism).flatMap(_.par.filter(predicate)))

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val schema: Seq[(String, LynxType)] = columns.map {
      case (name, expression) => name -> expressionEvaluator.typeOf(expression, df.schema.toMap)
    }

    val newSchema: Seq[(String, LynxType)] = df.schema.filterNot(col => schema.map(_._1).contains(col._1)) ++ schema
    val newColumns: Seq[(String, Expression)] = df.schema.filterNot(col => schema.map(_._1).contains(col._1))
      .map(col => (col._1, Variable(col._1)(InputPosition(0,0,0)))) ++ columns

    DataFrame(newSchema,
      () => df.records.map(
        record => {
          val recordCtx = ctx.withVars(df.columnsName.zip(record).toMap)
          newColumns.map(col => LazyLynxValue(() => expressionEvaluator.eval(col._2)(recordCtx))) //TODO: to opt
        }
      )
    )
  }


  override def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    // match (n:nothislabel) return count(n)
    val newSchema = (groupings ++ aggregations).map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, df.schema.toMap)
    )
    val columnsName = df.columnsName

    DataFrame(newSchema, () => {
      if (groupings.nonEmpty) {
        df.records.map { record =>
          val recordCtx = ctx.withVars(columnsName.zip(record).toMap)
          groupings.map(col => expressionEvaluator.eval(col._2)(recordCtx)) -> recordCtx
        } // (groupingValue: Seq[LynxValue] -> recordCtx: ExpressionContext)
          .toSeq.groupBy(_._1) // #group by 'groupingValue'.
          .mapValues(_.map(_._2)) // #trans to: (groupingValue: Seq[LynxValue] -> recordsCtx: Seq[ExpressionContext])
          .map { case (groupingValue, recordsCtx) => // #aggragate: (groupingValues & aggregationValues): Seq[LynxValue]
            groupingValue ++ {
              aggregations.map { case (name, expr) => expressionEvaluator.aggregateEval(expr)(recordsCtx.toIterator) }
            }
          }.toIterator
      } else {
        val allRecordsCtx = df.records.map { record => ctx.withVars(columnsName.zip(record).toMap) }
        Iterator(aggregations.map { case (name, expr) => expressionEvaluator.aggregateEval(expr)(allRecordsCtx) })
      }
    })
  }

  override def skip(df: DataFrame, num: Int): DataFrame =
    DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame = DataFrame(df.schema, () => df.records.take(num))

  override def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    // Select the connection algorithm based on heuristic rules
//    TODO need DataFrame but found  (DataFrame, DataFrame, Seq[String], JoinType) => DataFrame
//    JoinerSelector.chooseJoiner(a, b, joinColumns, joinType)
    SortMergeJoiner.join(a, b, joinColumns, joinType)
  }

  override def cross(a: DataFrame, b: DataFrame): DataFrame = {
    DataFrame(a.schema ++ b.schema, () => a.records.flatMap(ra => b.records.map(ra ++ _)))
  }

  /*
  * @param: df is a DataFrame
  * @function: Remove the duplicated rows in the df.
  * */
  override def distinct(df: DataFrame): DataFrame = DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)], limit: Expression, skip:Expression)(ctx: ExpressionContext): DataFrame = {
    val columnsName: Seq[String] = df.columnsName
    val sortColumns = sortItem.map(col => col._1 match {
      case Variable(expr) => (expr, if(col._2) -1 else 1)
      case _ => (col._1.toString, if(col._2) -1 else 1)
    })
    val newSchema = sortColumns.map(col => (col._1, LTAny)) ++ df.schema.filterNot(col => sortColumns.map(_._1).contains(col._1))
    val newDf = DataFrame(newSchema, () => {
      df.records.map(record => {
        val recordCtx = ctx.withVars(columnsName.zip(record).toMap)
        sortItem.map(col => expressionEvaluator.eval(col._1)(recordCtx)) ++ columnsName.zip(record).filterNot(col => sortColumns.map(_._1).contains(col._1)).map(_._2)
      })
    })
    expressionEvaluator.eval(limit)(ctx) match {
      case LynxInteger(n) => DataFrame(newSchema, ()=> {
        val asc: Seq[Int] = newSchema.map(_._1).map(col => sortColumns.toMap.getOrElse(col, 0))
        expressionEvaluator.eval(skip)(ctx) match {
          case LynxInteger(skipNum) => topNParallel(newDf.records, asc, n+skipNum).takeRight(n.toInt).toIterator
          case _ => topNParallel(newDf.records, asc, n).toIterator
        }
      })
      case _ => DataFrame(df.schema, () => df.records.toSeq
        .sortWith { (A, B) =>
          val ctxA = ctx.withVars(columnsName.zip(A).toMap) //map
          val ctxB = ctx.withVars(columnsName.zip(B).toMap)
          val sortValue = sortItem.map {
            case (exp, asc) =>
              (expressionEvaluator.eval(exp)(ctxA), expressionEvaluator.eval(exp)(ctxB), asc)
          }
          _ascCmp(sortValue.toIterator)
        }.toIterator)
    }
  }

  private def topNParallel(records: Iterator[Seq[LynxValue]], asc: Seq[Int], n: Long): Seq[Seq[LynxValue]] = {
    def compare(a: Seq[LynxValue], b: Seq[LynxValue]): Boolean = {
      val cmp = asc.iterator.zipWithIndex.foldLeft(0) { case (acc, (order, i)) =>
        if (acc != 0 || order == 0) acc // 如果已经比较出大小，直接返回
        else if (order == -1) a(i).compareTo(b(i)) // 升序
        else if (order == 1) -a(i).compareTo(b(i)) // 降序
        else 0 // order == 0，跳过此列
      }
      cmp < 0 // 负数表示 a < b，返回 true
    }
    implicit val ordering: Ordering[Seq[LynxValue]] = Ordering.fromLessThan(compare)

    val chunkSize = 50000 // 选择合适的分块大小
    // 处理每个块的 topN
    def processChunk(chunk: Seq[Seq[LynxValue]]): PriorityQueue[Seq[LynxValue]] = {
      val pq = PriorityQueue.empty[Seq[LynxValue]](ordering)
      chunk.par.foreach { record =>
        synchronized {
          pq.enqueue(record)
          if (pq.size > n) pq.dequeue()
        }
      }
      pq
    }

    def processParallelChunk(chunk: Seq[Seq[LynxValue]]): Seq[PriorityQueue[Seq[LynxValue]]] = {
      val numPartitions = 20
      val partitions: Seq[Seq[Seq[LynxValue]]] = chunk.grouped((chunk.size.toDouble / numPartitions).ceil.toInt).toSeq

      val f = Future.sequence(partitions.map { subChunk =>
        Future {
          val pq = PriorityQueue.empty[Seq[LynxValue]](ordering)
          subChunk.foreach { record =>
            pq.synchronized {
              pq.enqueue(record)
              if (pq.size > n) pq.dequeue()
            }
          }
          pq
        }
      })
      Await.result(f, Duration.Inf)
    }

    def mergePriorityQueue(sortedSeq: Seq[PriorityQueue[Seq[LynxValue]]]): mutable.PriorityQueue[Seq[LynxValue]] = {
      val finalHeap: mutable.PriorityQueue[Seq[LynxValue]] = PriorityQueue.empty[Seq[LynxValue]](ordering)
      sortedSeq.foreach(partialResults =>{
        partialResults.foreach(record => {
          finalHeap.enqueue(record)
          if (finalHeap.size > n) finalHeap.dequeue()
        })
      })
      finalHeap
    }

    // 分块并发处理，每个 Future 处理一个 chunk
    val chunkIter: Iterator[PriorityQueue[Seq[LynxValue]]] =
      records.grouped(chunkSize).map(chunk => mergePriorityQueue(processParallelChunk(chunk)))

    // 合并所有 Future 的结果
    val finalHeap: mutable.PriorityQueue[Seq[LynxValue]] = mergePriorityQueue(chunkIter.toSeq)
    finalHeap.toSeq.sorted(ordering)
  }

  private def _ascCmp(sortValue: Iterator[(LynxValue, LynxValue, Boolean)]): Boolean = {
    while (sortValue.hasNext) {
      val (valueOfA, valueOfB, asc) = sortValue.next()
      val comparable = valueOfA.compareTo(valueOfB)
      if(comparable != 0) return comparable > 0 != asc
    }
    false
  }

}
