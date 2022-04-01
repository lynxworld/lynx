package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types._
import org.grapheco.lynx.types.property.{LynxDouble, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.opencypher.v9_0.util.symbols.{CTAny, CTList, CypherType}

/**
 * @ClassName LynxList
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxList(v: List[LynxValue]) extends LynxCompositeValue {
  override def value: List[LynxValue] = v

  override def cypherType: CypherType = CTList(CTAny)

  /*
  - Any null values are excluded from the calculation.
  - In a mixed set, any numeric value is always considered to be higher than any string value, and any
  string value is always considered to be higher than any list.
  - Lists are compared in dictionary order, i.e. list elements are compared pairwise in ascending order
  from the start of the list to the end.
   */
  final val NUMERIC = 9
  final val STRING = 8
  final val LIST = 7

  val ordering: Ordering[LynxValue] = new Ordering[LynxValue] {
    def typeLevel(lynxValue: LynxValue): Int = lynxValue match {
      case _: LynxNumber => NUMERIC
      case _: LynxString => STRING
      case _: LynxList => LIST
      case _ => 0
    }

    def parseNumber(number: LynxNumber): Double = number match {
      case LynxInteger(i) => i.toDouble
      case LynxDouble(d) => d
    }

    override def compare(x: LynxValue, y: LynxValue): Int = {
      val x_level = typeLevel(x)
      val y_level = typeLevel(y)
      if (x_level == y_level) {
        (x, y) match {
          case (x: LynxNumber, y: LynxNumber) => parseNumber(x).compare(parseNumber(y))
          case (LynxString(str_x), LynxString(str_y)) => str_x.compare(str_y)
          case (LynxList(list_x), LynxList(list_y)) => {
            val iter_x = list_x.iterator
            val iter_y = list_y.iterator
            while (iter_x.hasNext && iter_y.hasNext) {
              val elementCompared = compare(iter_x.next(), iter_y.next())
              if (elementCompared != 0) return elementCompared
            }
            (iter_x.hasNext, iter_y.hasNext) match {
              case (true, false) => 1
              case (false, true) => -1
              case (false, false) => 0
            }
          }
          case (_, _) => 0 //TODO any other situations?
        }
      } else x_level - y_level
    }
  }

  lazy val droppedNull: Seq[LynxValue] = v.filterNot(LynxNull.equals)

  def min: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.min(ordering)

  def max: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.max(ordering)
}
