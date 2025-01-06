package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types._
import org.grapheco.lynx.types.property._

/**
 * @ClassName LynxList
 * @Description Represents a list of LynxValues with comparison logic.
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxList(v: List[LynxValue]) extends LynxCompositeValue {
  override def value: List[LynxValue] = v

  override def lynxType: ListType = LTList(LTAny)

  /**
   * Lists are compared in dictionary order, i.e., list elements are compared pairwise in
   * ascending order from the start of the list to the end. Elements missing in a shorter list are
   * considered to be less than any other value (including null values). For example, [1] < [1, 0]
   * but also [1] < [1, null].
   * If comparing two lists requires comparing at least a single null value to some other value,
   * these lists are incomparable. For example, [1, 2] >= [1, null] evaluates to null.
   * Lists are incomparable to any value that is not also a list.
   */
  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case l: LynxList =>
      val iterX = this.value.iterator
      val iterY = l.value.iterator
      while (iterX.hasNext && iterY.hasNext) {
        val elementCompared = iterX.next().compareTo(iterY.next())
        if (elementCompared != 0) return elementCompared
      }
      (iterX.hasNext, iterY.hasNext) match {
        case (true, false) => 1
        case (false, true) => -1
        case (false, false) => 0
      }
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }

  /**
   * Filters out null values from the list.
   */
  lazy val droppedNull: Seq[LynxValue] = v.filterNot(LynxNull.equals)

  /**
   * Applies a function to each element in the list and returns a new LynxList.
   * @param f The function to apply.
   * @return A new LynxList with the function applied to each element.
   */
  def map(f: LynxValue => LynxValue): LynxList = LynxList(this.v.map(f))

  /**
   * Returns the minimum value in the list, ignoring null values.
   * @return The minimum value or LynxNull if the list is empty or contains only null values.
   */
  def min: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.min

  /**
   * Returns the maximum value in the list, ignoring null values.
   * @return The maximum value or LynxNull if the list is empty or contains only null values.
   */
  def max: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.max
}