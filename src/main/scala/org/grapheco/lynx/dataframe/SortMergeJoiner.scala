package org.grapheco.lynx.dataframe

import org.grapheco.lynx.LynxException
import org.grapheco.lynx.types.{LynxType, LynxValue}
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.util.Profiler

import scala.collection.mutable.ListBuffer

/**
 * Sort-Merge Join implementation supporting multiple join types
 */

object SortMergeJoiner {
  /**
   * Public join method that handles all join types
   * @param a Left DataFrame
   * @param b Right DataFrame
   * @param joinColumns Columns to join on
   * @param joinType Type of join to perform
   * @return Joined DataFrame
   */
  def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    // Validate join columns exist in both DataFrames
    val missingColumns = joinColumns.filter { col =>
      !a.columnsName.contains(col) || !b.columnsName.contains(col)
    }
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Join columns not found in both DataFrames: ${missingColumns.mkString(", ")}")
    }

    val joinColIndexs = joinColumns.map(col =>
      (a.columnsName.indexOf(col), b.columnsName.indexOf(col))
    )

    // Prepare schema with proper handling of duplicate column names
    val (joinedSchema, renameRightColumns) = prepareJoinedSchema(a, b)

    // Sort both DataFrames on join columns (ascending order)
    val sortedA = sortDataFrame(a, joinColIndexs.map(_._1))
    val sortedB = sortDataFrame(b, joinColIndexs.map(_._2))

    // Perform the appropriate join type
    val joinedData = joinType match {
      case InnerJoin | LeftJoin | RightJoin | OuterJoin => mergeJoin(sortedA, sortedB,a.schema.size, b.schema.size, joinColIndexs, joinType, renameRightColumns)
      case _ => throw LynxException(s"Join type $joinType is not supported")
    }

    DataFrame(joinedSchema, () => joinedData.toIterator)
  }

  /**
   * Prepares joined schema with proper handling of duplicate column names
   */
  private def prepareJoinedSchema(a: DataFrame, b: DataFrame): (Seq[(String, LynxType)], Boolean) = {
    val aColumns = a.schema.map(_._1).toSet
    val hasDuplicateColumns = b.schema.exists { case (name, _) => aColumns.contains(name) }

    if (hasDuplicateColumns) {
      val newBSchema = b.schema.map { case (name, typ) =>
        if (aColumns.contains(name)) (s"right.$name", typ) else (name, typ)
      }
      (a.schema ++ newBSchema, true)
    } else {
      (a.schema ++ b.schema, false)
    }
  }

  /**
   * Sorts a DataFrame based on specified column indices
   */
  private def sortDataFrame(dataFrame: DataFrame, sortColIndexs: Seq[Int]): Array[Seq[LynxValue]] = {
      dataFrame.records.toArray.sortWith((rowA, rowB) => compareRows(rowA, rowB, sortColIndexs) < 0)
  }

  /**
   * Compares two rows based on specified columns
   * @return negative if rowA < rowB, positive if rowA > rowB, 0 if equal
   */
  private def compareRows(rowA: Seq[LynxValue], rowB: Seq[LynxValue], compareColIndexs: Seq[Int]): Int = {
    compareColIndexs.view
      .map(idx => rowA(idx).compareTo(rowB(idx)))
      .find(_ != 0)
      .getOrElse(0)
  }

  /**
   * Core merge join implementation that handles all join types
   */
  private def mergeJoin(
    sortedA: Array[Seq[LynxValue]],
    sortedB: Array[Seq[LynxValue]],
    aColCount: Int,
    bColCount: Int,
    joinColIndexs: Seq[(Int, Int)],
    joinType: JoinType,
    renameRightColumns: Boolean
  ): ListBuffer[Seq[LynxValue]] = {
    val result = ListBuffer[Seq[LynxValue]]()
    var i = 0 // Index for sortedA
    var j = 0 // Index for sortedB
//    val aColCount = sortedA.schema.length
//    val bColCount = sortedB.schema.length

    while (i < sortedA.length && j < sortedB.length) {
      val currentA = sortedA(i)
      val currentB = sortedB(j)
      val aJoinKey = joinColIndexs.map(_._1).map(currentA)
      val bJoinKey = joinColIndexs.map(_._2).map(currentB)

      val comparison = compareJoinKeys(aJoinKey, bJoinKey)

      comparison match {
        case 0 => // Keys match - perform join
          val aGroup = collectGroup(sortedA, i, joinColIndexs.map(_._1))
          val bGroup = collectGroup(sortedB, j, joinColIndexs.map(_._2))

          // Cross product for matching groups
          aGroup.foreach(aRow =>
            bGroup.foreach(bRow => result += (aRow ++ bRow))
          )

          i += aGroup.size
          j += bGroup.size

        case -1 => // aKey < bKey - handle left-only rows
          if (joinType == LeftJoin || joinType == OuterJoin) {
            val aGroup = collectGroup(sortedA, i, joinColIndexs.map(_._1))
            aGroup.foreach(aRow => result += (aRow ++ Seq.fill(bColCount)(LynxNull)))
            i += aGroup.size
          } else {
            i += 1
          }

        case 1 => // aKey > bKey - handle right-only rows
          if (joinType == RightJoin || joinType == OuterJoin) {
            val bGroup = collectGroup(sortedB, j, joinColIndexs.map(_._2))
            bGroup.foreach(bRow => result += (Seq.fill(aColCount)(LynxNull) ++ bRow))
            j += bGroup.size
          } else {
            j += 1
          }
      }
    }

    // Handle remaining rows
    handleRemainingRows(sortedA, i, result, aColCount, bColCount, joinType, isLeft = true)
    handleRemainingRows(sortedB, j, result, aColCount, bColCount, joinType, isLeft = false)

    result
  }

  /**
   * Compare two join keys
   */
  private def compareJoinKeys(aKey: Seq[LynxValue], bKey: Seq[LynxValue]): Int = {
    aKey.zip(bKey)
      .map { case (a, b) => a.compareTo(b) }
      .find(_ != 0)
      .getOrElse(0)
  }

  /**
   * Collect all consecutive rows with the same join key
   */
  private def collectGroup(
    sortedData: Array[Seq[LynxValue]],
    startIndex: Int,
    joinColIndices: Seq[Int]
  ): Seq[Seq[LynxValue]] = {
    if (startIndex >= sortedData.length) return Seq.empty

    val group = ListBuffer[Seq[LynxValue]]()
    val key = joinColIndices.map(sortedData(startIndex)(_))
    var i = startIndex

    while (i < sortedData.length) {
      val currentRow = sortedData(i)
      val currentKey = joinColIndices.map(currentRow(_))

      if (currentKey == key) {
        group += currentRow
        i += 1
      } else {
        i = sortedData.length // Exit loop
      }
    }

    group
  }

  /**
   * Handle remaining rows after main merge loop
   */
  private def handleRemainingRows(
    sortedData: Array[Seq[LynxValue]],
    startIndex: Int,
    result: ListBuffer[Seq[LynxValue]],
    aColCount: Int,
    bColCount: Int,
    joinType: JoinType,
    isLeft: Boolean
  ): Unit = {
    val shouldAdd = (isLeft && (joinType == LeftJoin || joinType == OuterJoin)) ||
                   (!isLeft && (joinType == RightJoin || joinType == OuterJoin))

    if (shouldAdd) {
      for (i <- startIndex until sortedData.length) {
        val row = sortedData(i)
        if (isLeft) {
          result += (row ++ Seq.fill(bColCount)(LynxNull))
        } else {
          result += (Seq.fill(aColCount)(LynxNull) ++ row)
        }
      }
    }
  }
}