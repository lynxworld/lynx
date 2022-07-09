package org.grapheco

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, Joiner}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.junit.Test

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:29 2022/7/8
 * @Modified By:
 */
class TempTest {
  @Test
  def test1(): Unit = {
    val row1: Seq[String] = Seq("a", "a", "b")
    val row2: Seq[String] = Seq("a", "a", "a")
    val cmpIndexs: Seq[Int] = Seq(0, 1,2)
    println(_rowCmpGreater(row1, row2, cmpIndexs))
  }

  @Test
  def test2(): Unit = {
    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5)
    println(seq.map(item=> if(item>3) item))
  }

  @Test
  def test3(): Unit = {
    val schemaA: Seq[(String, LynxType)] = Seq(("id", LynxInteger(1).lynxType), ("name", LynxString("").lynxType))
    val schemaB: Seq[(String, LynxType)] = Seq(("id", LynxInteger(1).lynxType), ("value", LynxInteger(1).lynxType), ("date", LynxString("").lynxType))

    val tableA: List[Seq[LynxValue]] = List(
      Seq(LynxInteger(100), LynxString("Andy")),
      Seq(LynxInteger(200), LynxString("GZA")),
      Seq(LynxInteger(200), LynxString("GZA"))
    )

    val tableB: List[Seq[LynxValue]] = List(
      Seq(LynxInteger(100), LynxInteger(2222), LynxString("10/2/2019")),
      Seq(LynxInteger(100), LynxInteger(9999), LynxString("10/2/2019")),
      Seq(LynxInteger(200), LynxInteger(8888), LynxString("10/2/2019")),
      Seq(LynxInteger(400), LynxInteger(6666), LynxString("10/2/2019"))
    )

    val tableC: List[Seq[LynxValue]] = new Array[Int](1000000).map(_ => Seq(LynxInteger(200), LynxString("GZA"))).toList
    val tableD: List[Seq[LynxValue]] = new Array[Int](1000000).map(_ => Seq(LynxInteger(100), LynxInteger(2222), LynxString("10/2/2019"))).toList

    val dfA: DataFrame = DataFrame(schemaA, () => tableA.toIterator)
    val dfB: DataFrame = DataFrame(schemaB, () => tableB.toIterator)
    val dfC: DataFrame = DataFrame(schemaA, () => tableC.toIterator)
    val dfD: DataFrame = DataFrame(schemaB, () => tableD.toIterator)

    val result = Joiner.join(dfC, dfD, Seq("id"), InnerJoin)
//    result.records.foreach(println)
  }

  private def _rowCmpGreater(row1: Seq[String], row2: Seq[String], cmpColIndexs: Seq[Int]): Boolean = {
    if(cmpColIndexs.length > 0) {
      val valueA = row1(cmpColIndexs.head)
      val valueB = row2(cmpColIndexs.head)
      (valueA, valueA) match {
        case cmp if(valueA > valueB) => true
        case cmp if(valueA < valueB) => false
        case cmp if(valueA == valueB) => _rowCmpGreater(row1, row2, cmpColIndexs.drop(1))
      }
    } else false
  }
}
