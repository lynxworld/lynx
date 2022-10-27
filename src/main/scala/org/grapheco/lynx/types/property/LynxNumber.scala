package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}

trait LynxNumber extends LynxValue {
  def number: Number

  def +(that: LynxNumber): LynxNumber

  def -(that: LynxNumber): LynxNumber

  def *(that: LynxNumber): LynxNumber

  def /(that: LynxNumber): LynxNumber

  def toDouble: Double = this match {
    case LynxInteger(i) => i.toDouble
    case LynxFloat(d) => d
  }

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case n: LynxNumber => toDouble.compareTo(n.toDouble)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
