package org.grapheco.lynx.dataframe

import org.grapheco.lynx.types.{LynxType, LynxValue}
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.util.Profiler

import scala.collection.mutable.{ListBuffer, HashMap}

object HashJoiner {

  def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    val joinColIndexes: Seq[(Int, Int)] = joinColumns
      .map(columnName =>
        (a.columnsName.indexOf(columnName), b.columnsName.indexOf(columnName))
      )

    joinType match {
      case InnerJoin => _innerJoin(a, b, joinColIndexes)
      case OuterJoin => _fullOuterJoin(a, b, joinColIndexes)
      case LeftJoin => _leftJoin(a, b, joinColIndexes)
      case RightJoin => _rightJoin(a, b, joinColIndexes)
      case _ => throw new Exception("Unexpected JoinType in DataFrame Join Function.")
    }
  }

  private def _innerJoin(a: DataFrame, b: DataFrame, joinColIndexes: Seq[(Int, Int)]): DataFrame = {
    val hashTableB: HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = _buildHashTable(b, joinColIndexes.map(_._2))

    val joinedSchema: Seq[(String, LynxType)] = a.schema ++ b.schema
    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    for (rowA <- a.records) {
      val joinKeyA: Seq[LynxValue] = joinColIndexes.map { case (aIndex, _) => rowA(aIndex) }
      if (hashTableB.contains(joinKeyA)) {
        hashTableB(joinKeyA).foreach { rowB =>
          joinedDataFrame.append(rowA ++ rowB)
        }
      }
    }

    DataFrame(joinedSchema, () => joinedDataFrame.toIterator)
  }

  private def _fullOuterJoin(a: DataFrame, b: DataFrame, joinColIndexes: Seq[(Int, Int)]): DataFrame = {
    val hashTableB: HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = _buildHashTable(b, joinColIndexes.map(_._2))

    val joinedSchema: Seq[(String, LynxType)] = a.schema ++ b.schema
    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    for (rowA <- a.records) {
      val joinKeyA: Seq[LynxValue] = joinColIndexes.map { case (aIndex, _) => rowA(aIndex) }
      if (hashTableB.contains(joinKeyA)) {
        hashTableB(joinKeyA).foreach { rowB =>
          joinedDataFrame.append(rowA ++ rowB)
        }
      } else {
        // No match in B, so append rowA with nulls for B's columns
        joinedDataFrame.append(rowA ++ new Array[LynxValue](b.schema.length).map(_ => LynxNull))
      }
    }

    // Add rows from B that don't have a match in A
    for (rowB <- b.records) {
      val joinKeyB: Seq[LynxValue] = joinColIndexes.map { case (_, bIndex) => rowB(bIndex) }
      val matchingRowsInA = a.records.exists { rowA =>
        val joinKeyA: Seq[LynxValue] = joinColIndexes.map { case (aIndex, _) => rowA(aIndex) }
        joinKeyA == joinKeyB
      }
      if (!matchingRowsInA) {
        // No match in A, so append rowB with nulls for A's columns
        joinedDataFrame.append(new Array[LynxValue](a.schema.length).map(_ => LynxNull) ++ rowB)
      }
    }

    DataFrame(joinedSchema, () => joinedDataFrame.toIterator)
  }

  private def _leftJoin(a: DataFrame, b: DataFrame, joinColIndexes: Seq[(Int, Int)]): DataFrame = {
    val hashTableB: HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = _buildHashTable(b, joinColIndexes.map(_._2))

    val joinedSchema: Seq[(String, LynxType)] = a.schema ++ b.schema
    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    for (rowA <- a.records) {
      val joinKeyA: Seq[LynxValue] = joinColIndexes.map { case (aIndex, _) => rowA(aIndex) }
      if (hashTableB.contains(joinKeyA)) {
        hashTableB(joinKeyA).foreach { rowB =>
          joinedDataFrame.append(rowA ++ rowB)
        }
      } else {
        // No match in B, so append rowA with nulls for B's columns
        joinedDataFrame.append(rowA ++ new Array[LynxValue](b.schema.length).map(_ => LynxNull))
      }
    }

    DataFrame(joinedSchema, () => joinedDataFrame.toIterator)
  }

  private def _rightJoin(a: DataFrame, b: DataFrame, joinColIndexes: Seq[(Int, Int)]): DataFrame = {
    val hashTableA: HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = _buildHashTable(a, joinColIndexes.map(_._1))

    val joinedSchema: Seq[(String, LynxType)] = a.schema ++ b.schema
    val joinedDataFrame: ListBuffer[Seq[LynxValue]] = ListBuffer[Seq[LynxValue]]()

    for (rowB <- b.records) {
      val joinKeyB: Seq[LynxValue] = joinColIndexes.map { case (_, bIndex) => rowB(bIndex) }
      if (hashTableA.contains(joinKeyB)) {
        hashTableA(joinKeyB).foreach { rowA =>
          joinedDataFrame.append(rowA ++ rowB)
        }
      } else {
        // No match in A, so append rowB with nulls for A's columns
        joinedDataFrame.append(new Array[LynxValue](a.schema.length).map(_ => LynxNull) ++ rowB)
      }
    }

    DataFrame(joinedSchema, () => joinedDataFrame.toIterator)
  }

  private def _buildHashTable(df: DataFrame, joinColIndexes: Seq[Int]): HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = {
    val hashTable: HashMap[Seq[LynxValue], ListBuffer[Seq[LynxValue]]] = HashMap.empty

    for (row <- df.records) {
      val joinKey: Seq[LynxValue] = joinColIndexes.map(colIndex => row(colIndex))
      if (!hashTable.contains(joinKey)) {
        hashTable(joinKey) = ListBuffer[Seq[LynxValue]]()
      }
      hashTable(joinKey).append(row)
    }

    hashTable
  }

}
