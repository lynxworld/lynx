
package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types._
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxNumber, LynxString}

/**
 * Represents a list of LynxValue in the Lynx graph database.
 * @param v The list of LynxValue elements.
 */
case class LynxList private (v: List[LynxValue]) extends LynxCompositeValue {

  /**
   * Returns the underlying list of LynxValue elements.
   */
  override def value: List[LynxValue] = v

  /**
   * Returns the type of this LynxList, which is a list of any type (LTAny).
   */
  override def lynxType: ListType = LTList(LTAny)

  /**
   * Compares this LynxList with another LynxValue in dictionary order.
   * @param o The LynxValue to compare with.
   * @return An integer indicating the comparison result.
   */
  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case l: LynxList =>
      compareLists(this.value, l.value)
    case _ =>
      throw TypeMismatchException(this.lynxType, o.lynxType)
  }

  /**
   * Filters out LynxNull values from the list.
   */
  lazy val droppedNull: Seq[LynxValue] = v.filterNot(_ == LynxNull)

  /**
   * Applies a function to each element of the list and returns a new LynxList.
   * @param f The function to apply.
   * @return A new LynxList with the function applied to each element.
   */
  def map(f: LynxValue => LynxValue): LynxList = LynxList(v.map(f))

  /**
   * Finds the minimum LynxValue in the list, excluding LynxNull values.
   * @return The minimum LynxValue or LynxNull if the list is empty.
   */
  def min: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.min

  /**
   * Finds the maximum LynxValue in the list, excluding LynxNull values.
   * @return The maximum LynxValue or LynxNull if the list is empty.
   */
  def max: LynxValue = if (droppedNull.isEmpty) LynxNull else droppedNull.max

  /**
   * Helper method to compare two lists in dictionary order.
   */
  private def compareLists(list1: List[LynxValue], list2: List[LynxValue]): Int = {
    val iter1 = list1.iterator
    val iter2 = list2.iterator
    while (iter1.hasNext && iter2.hasNext) {
      val compared = iter1.next().compareTo(iter2.next())
      if (compared != 0) return compared
    }
    (iter1.hasNext, iter2.hasNext) match {
      case (true, false) => 1
      case (false, true) => -1
      case (false, false) => 0
    }
  }
}

