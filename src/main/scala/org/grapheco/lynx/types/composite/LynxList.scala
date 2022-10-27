package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types._
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxNumber, LynxString}
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

  override def lynxType: CypherType = CTList(CTAny)

  /*
  Lists are compared in dictionary order, i.e. list elements are compared pairwise in
  ascending order from the start of the list to the end. Elements missing in a shorter list are
  considered to be less than any other value (including null values). For example, [1] < [1, 0]
  but also [1] < [1, null].
  • If comparing two lists requires comparing at least a single null value to some other value,
  these lists are incomparable. For example, [1, 2] >= [1, null] evaluates to null.
  • Lists are incomparable to any value that is not also a list.
   */
  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case l: LynxList =>
      val iter_x = this.value.iterator
      val iter_y = l.value.iterator
      while (iter_x.hasNext && iter_y.hasNext) {
        val elementCompared = iter_x.next().compareTo(iter_y.next())
        if (elementCompared != 0) return elementCompared
      }
      (iter_x.hasNext, iter_y.hasNext) match {
        case (true, false) => 1
        case (false, true) => -1
        case (false, false) => 0
      }
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }

  lazy val droppedNull: Seq[LynxValue] = v.filterNot(LynxNull.equals)

  def map(f: LynxValue => LynxValue): LynxList = LynxList(this.v.map(f))


  def min: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.min

  def max: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.max
}
