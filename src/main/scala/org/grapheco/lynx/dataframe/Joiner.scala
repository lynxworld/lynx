package org.grapheco.lynx.dataframe

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.util.Profiler

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
    //      a.schema ++ b.schema.filterNot(column => joinColumns.contains(column._1))

    joinType match {
      case InnerJoin => DataFrame(joinedSchema, _innerJoin(a, b, joinColIndexs))
      case _ => throw new Exception("UnExpected JoinType in DataFrame Join Function.")
    }
  }

  private def _innerJoin(a: DataFrame, b: DataFrame, joinColIndexs: Seq[(Int, Int)]): () => Iterator[Seq[LynxValue]] = {
//    val sortedTableA: List[Seq[LynxValue]] = Profiler.timing("SortA", a.records.toList.sortBy(_.apply(joinColIndexs.head._1))(LynxValue.ordering))
//    val sortedTableB: List[Seq[LynxValue]] = Profiler.timing("SortB", b.records.toList.sortBy(_.apply(joinColIndexs.head._2))(LynxValue.ordering))
    val sortedTableA: List[Seq[LynxValue]] = a.records.toList
    val sortedTableB: List[Seq[LynxValue]] = b.records.toList

// TODO: Make full use of the sortedTableB
    () => sortedTableA.flatMap(rowA =>{
      val joinedColA: Seq[LynxValue] = joinColIndexs.map(index => rowA(index._1))
      val joinableRowsInB: List[Seq[LynxValue]] = sortedTableB.filter(rowB => {
        val joinedColB: Seq[LynxValue] = joinColIndexs.map(index => rowB(index._2))
        joinedColB.zip(joinedColA).forall(pair => pair._1.compareTo(pair._2) == 0)
      }
      )

      joinableRowsInB.map(rowB => rowA ++ rowB)
    }).toIterator
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

}
