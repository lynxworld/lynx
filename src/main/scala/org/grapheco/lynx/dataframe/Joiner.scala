package org.grapheco.lynx.dataframe

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
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
      case OuterJoin => DataFrame(joinedSchema, _fullOuterJoin(a, b, joinColIndexs))
      case LeftJoin => DataFrame(joinedSchema, _leftJoin(a, b, joinColIndexs))
      case RightJoin => DataFrame(joinedSchema, _rightJoin(a, b, joinColIndexs))
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
        _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1))
      val nextInnerRowsB: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2))

      val status: Int = _innerMergeRows(nextInnerRowsA, nextInnerRowsB, joinColIndexs, joinedDataFrame)
      if (status == 0) {
        indexOfA += nextInnerRowsA.length
        indexOfB += nextInnerRowsB.length
      } else if (status > 0) indexOfB += nextInnerRowsB.length
      else indexOfA += nextInnerRowsA.length
    }

    () => joinedDataFrame.toIterator
  }

  private def _fullOuterJoin(a: DataFrame, b: DataFrame, joinColIndexs: Seq[(Int, Int)]): () => Iterator[Seq[LynxValue]] = {
    // Is this asending or desending?
    val sortedTableA: Array[Seq[LynxValue]] = Profiler.timing("SortA", a.records.toArray.sortBy(_.apply(joinColIndexs.head._1))(LynxValue.ordering))
    val sortedTableB: Array[Seq[LynxValue]] = Profiler.timing("SortB", b.records.toArray.sortBy(_.apply(joinColIndexs.head._2))(LynxValue.ordering))

    var indexOfA: Int = 0
    var indexOfB: Int = 0

    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    while (indexOfA < sortedTableA.length && indexOfB < sortedTableB.length) {
      val nextInnerRowsA: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1))
      val nextInnerRowsB: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2))

      val status: Int = _fullOuterMergeRows(nextInnerRowsA, nextInnerRowsB, joinColIndexs, joinedDataFrame)
      if (status == 0) {
        indexOfA += nextInnerRowsA.length
        indexOfB += nextInnerRowsB.length
      } else if (status > 0) {
        indexOfB += nextInnerRowsB.length
      } else {
        indexOfA += nextInnerRowsA.length
      }
    }

    while (indexOfA < sortedTableA.length) {
      joinedDataFrame.append(sortedTableA(indexOfA) ++ new Array[Int](sortedTableB.head.length).map(_ => LynxNull))
      indexOfA += 1
    }
    while (indexOfB < sortedTableB.length) {
      joinedDataFrame.append(new Array[Int](sortedTableA.head.length).map(_ => LynxNull) ++ sortedTableB(indexOfB))
      indexOfB += 1
    }

    () => joinedDataFrame.toIterator
  }

  private def _leftJoin(a: DataFrame, b: DataFrame, joinColIndexs: Seq[(Int, Int)]): () => Iterator[Seq[LynxValue]] = {
    // Is this asending or desending?
    val sortedTableA: Array[Seq[LynxValue]] = Profiler.timing("SortA", a.records.toArray.sortBy(_.apply(joinColIndexs.head._1))(LynxValue.ordering))
    val sortedTableB: Array[Seq[LynxValue]] = Profiler.timing("SortB", b.records.toArray.sortBy(_.apply(joinColIndexs.head._2))(LynxValue.ordering))

    var indexOfA: Int = 0
    var indexOfB: Int = 0

    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    while (indexOfA < sortedTableA.length && indexOfB < sortedTableB.length) {
      val nextInnerRowsA: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1))
      val nextInnerRowsB: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2))

      val status: Int = _leftOuterMergeRows(nextInnerRowsA, nextInnerRowsB, joinColIndexs, joinedDataFrame)
      if (status == 0) {
        indexOfA += nextInnerRowsA.length
        indexOfB += nextInnerRowsB.length
      } else if (status > 0) indexOfB += nextInnerRowsB.length
      else indexOfA += nextInnerRowsA.length
    }

    while (indexOfA < sortedTableA.length) {
      joinedDataFrame.append(sortedTableA(indexOfA) ++ new Array[Int](sortedTableB.head.length).map(_ => LynxNull))
      indexOfA += 1
    }

    () => joinedDataFrame.toIterator
  }

  private def _rightJoin(a: DataFrame, b: DataFrame, joinColIndexs: Seq[(Int, Int)]): () => Iterator[Seq[LynxValue]] = {
    val sortedTableA: Array[Seq[LynxValue]] = Profiler.timing("SortA", a.records.toArray.sortBy(_.apply(joinColIndexs.head._1))(LynxValue.ordering))
    val sortedTableB: Array[Seq[LynxValue]] = Profiler.timing("SortB", b.records.toArray.sortBy(_.apply(joinColIndexs.head._2))(LynxValue.ordering))

    var indexOfA: Int = 0
    var indexOfB: Int = 0

    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    while (indexOfA < sortedTableA.length && indexOfB < sortedTableB.length) {
      val nextInnerRowsA: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableA, indexOfA, joinColIndexs.map(_._1))
      val nextInnerRowsB: Seq[Seq[LynxValue]] =
        _fetchNextInnerRows(sortedTableB, indexOfB, joinColIndexs.map(_._2))

      val status: Int = _rightOuterMergeRows(nextInnerRowsA, nextInnerRowsB, joinColIndexs, joinedDataFrame)
      if (status == 0) {
        indexOfA += nextInnerRowsA.length
        indexOfB += nextInnerRowsB.length
      } else if (status > 0) indexOfB += nextInnerRowsB.length
      else indexOfA += nextInnerRowsA.length
    }

    while (indexOfB < sortedTableB.length) {
      joinedDataFrame.append(new Array[Int](sortedTableA.head.length).map(_ => LynxNull) ++ sortedTableB(indexOfB))
      indexOfB += 1
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

  private def _fullOuterMergeRows(rowsA: Seq[Seq[LynxValue]], rowsB: Seq[Seq[LynxValue]],
                               joinColIndexs: Seq[(Int, Int)], joinedDataFrame: ListBuffer[Seq[LynxValue]]): Int = {
    val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowsA.head(index._1))
    val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowsB.head(index._2))
    if (joinedColA.zip(joinedColB).forall(pair => pair._1.compareTo(pair._2) == 0)) {
      rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB)))
      0
    } else {
      val compareResult: Int = joinedColA.zip(joinedColB).map(pair => pair._1.compareTo(pair._2)).filterNot(compareResult => compareResult == 0).head
      if (compareResult < 0) {
        rowsA.foreach(rowA => joinedDataFrame.append(rowA ++ new Array[Int](rowsB.head.length).map(_ => LynxNull)))
      } else {
        rowsB.foreach(rowB => joinedDataFrame.append(new Array[Int](rowsA.head.length).map(_ => LynxNull) ++ rowB))
      }
      compareResult
    }
  }

  private def _leftOuterMergeRows(rowsA: Seq[Seq[LynxValue]], rowsB: Seq[Seq[LynxValue]],
                                  joinColIndexs: Seq[(Int, Int)], joinedDataFrame: ListBuffer[Seq[LynxValue]]): Int = {
    val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowsA.head(index._1))
    val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowsB.head(index._2))
    if (joinedColA.zip(joinedColB).forall(pair => pair._1.compareTo(pair._2) == 0)) {
      rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB)))
      0
    } else {
      val compareResult: Int = joinedColA.zip(joinedColB).map(pair => pair._1.compareTo(pair._2)).filterNot(compareResult => compareResult == 0).head
      if (compareResult < 0) {
        rowsA.foreach(rowA => joinedDataFrame.append(rowA ++ new Array[Int](rowsB.head.length).map(_ => LynxNull)))
      }
      compareResult
    }
  }

  private def _rightOuterMergeRows(rowsA: Seq[Seq[LynxValue]], rowsB: Seq[Seq[LynxValue]],
                                   joinColIndexs: Seq[(Int, Int)], joinedDataFrame: ListBuffer[Seq[LynxValue]]): Int = {
    val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowsA.head(index._1))
    val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowsB.head(index._2))
    if (joinedColA.zip(joinedColB).forall(pair => pair._1.compareTo(pair._2) == 0)) {
      rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB)))
      0
    } else {
      val compareResult: Int = joinedColA.zip(joinedColB).map(pair => pair._1.compareTo(pair._2)).filterNot(compareResult => compareResult == 0).head
      if (compareResult > 0) {
        rowsB.foreach(rowB => joinedDataFrame.append(new Array[Int](rowsA.head.length).map(_ => LynxNull) ++ rowB))
      }
      compareResult
    }
  }

  private def _innerMergeRows(rowsA: Seq[Seq[LynxValue]], rowsB: Seq[Seq[LynxValue]],
                              joinColIndexs: Seq[(Int, Int)], joinedDataFrame: ListBuffer[Seq[LynxValue]]): Int = {
    val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowsA.head(index._1))
    val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowsB.head(index._2))

    if (joinedColA.zip(joinedColB).forall(pair => pair._1.compareTo(pair._2) == 0)) {
      rowsA.foreach(rowA => rowsB.foreach(rowB => joinedDataFrame.append(rowA ++ rowB)))
      0
    } else joinedColA.zip(joinedColB).map(pair => pair._1.compareTo(pair._2)).filterNot(compareResult => compareResult == 0).head
  }

  /*
      This func is to fetch rows from DataFrame, until a different join-keys
       */
  private def _fetchNextInnerRows(outerDataTable: Seq[Seq[LynxValue]], startIndex: Int, joinCols: Seq[Int]): Seq[Seq[LynxValue]] = {
    if(startIndex < outerDataTable.length) {
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
    } else Seq[Seq[LynxValue]]()
  }


}
