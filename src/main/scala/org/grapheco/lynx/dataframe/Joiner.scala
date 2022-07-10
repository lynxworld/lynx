package org.grapheco.lynx.dataframe

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.util.Profiler

import scala.collection.mutable.ListBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:34 2022/7/8
 * @Modified By:
 */

object Joiner {

  def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    val joinColIndexs: Seq[(Int, Int)] = joinColumns
      .map(columnName =>
        (a.columnsName.indexOf(columnName), b.columnsName.indexOf(columnName))
      )

    val joinedSchema: Seq[(String, LynxType)] = a.schema ++ b.schema

    joinType match {
      case InnerJoin => DataFrame(joinedSchema, _innerJoin(a, b, joinColIndexs))
      case _ => throw new Exception("UnExpected JoinType in DataFrame Join Function.")
    }
  }

  // Implamented as Sort-merge Join.
  private def _innerJoin(a: DataFrame, b: DataFrame, joinColIndexs: Seq[(Int, Int)]): () => Iterator[Seq[LynxValue]] = {
    // Is this asending or desending?
    val sortedTableA: Array[Seq[LynxValue]] = Profiler.timing("SortA", a.records.toArray.sortBy(_.apply(joinColIndexs.head._1))(LynxValue.ordering))
    val sortedTableB: Array[Seq[LynxValue]] = Profiler.timing("SortB", b.records.toArray.sortBy(_.apply(joinColIndexs.head._2))(LynxValue.ordering))

    var indexOfA: Int = 0
    var indexOfB: Int = 0

    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    while (indexOfA < sortedTableA.length && indexOfB < sortedTableB.length) {
      val nextInnerRowsA: Seq[Seq[LynxValue]] =
//        Profiler.timing("Fetching A ", _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1)))
        _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1))
      val nextInnerRowsB: Seq[Seq[LynxValue]] =
//        Profiler.timing("Fetching B", _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2)))
        _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2))

      val status: Int = _mergeInnerRows(nextInnerRowsA, nextInnerRowsB, joinColIndexs, joinedDataFrame)
      if (status == 0) {
        indexOfA += nextInnerRowsA.length
        indexOfB += nextInnerRowsB.length
      } else if (status > 0) indexOfB += nextInnerRowsB.length
      else indexOfA += nextInnerRowsA.length
    }

    () => joinedDataFrame.toIterator
  }

  // Compare Row at the specific columns.
  // This function is for SortJoin.
  private def _rowCmpGreater(row1: Seq[LynxValue], row2: Seq[LynxValue], cmpColIndexs: Seq[Int]): Boolean = {
    if(cmpColIndexs.length > 0) {
      val valueA = row1(cmpColIndexs.head)
      val valueB = row2(cmpColIndexs.head)
      valueA.compareTo(valueB) match {
        case result if (result > 0) => true
        case result if (result < 0)  => false
        case result if (result == 0) => _rowCmpGreater(row1, row2, cmpColIndexs.drop(1))
      }
    } else false
  }

  private def _mergeInnerRows(rowsA: Seq[Seq[LynxValue]], rowsB: Seq[Seq[LynxValue]],
                              joinColIndexs: Seq[(Int, Int)], joinedDataFrame: ListBuffer[Seq[LynxValue]]): Int = {
    val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowsA.head(index._1))
    val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowsB.head(index._2))

    if (joinedColA.head.compareTo(joinedColB.head) == 0) {
      rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB)))
//      Profiler.timing("Long Long Foreach", rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB))))
      0
    } else joinedColA.head.compareTo(joinedColB.head)
  }

  /*
      This func is to fetch rows from DataFrame, until a different join-keys
       */
  private def _fetchNextInnerRows(outerDataTable: Seq[Seq[LynxValue]], startIndex: Int, joinCols: Seq[Int]): Seq[Seq[LynxValue]] = {
    val rowsBuffer: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    val innerRowsJoinValue: Seq[LynxValue] = joinCols.map(colIndex => outerDataTable(startIndex)(colIndex))
    var innerIndex: Int = startIndex
    while (innerIndex < outerDataTable.length) {
      val row: Seq[LynxValue] = outerDataTable(innerIndex)
      if (joinCols.map(colIndex => row(colIndex)).zip(innerRowsJoinValue).forall(pair => pair._1.compareTo(pair._2) == 0)) {
        rowsBuffer.append(row)
        innerIndex += 1
      } else innerIndex = outerDataTable.length
    }
    rowsBuffer.toSeq
  }


}
