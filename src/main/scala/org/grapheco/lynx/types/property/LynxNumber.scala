package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}

trait LynxNumber extends LynxValue {
  def number: Number

  def +(that: LynxNumber): LynxNumber

  def -(that: LynxNumber): LynxNumber

  def *(that: LynxNumber): LynxNumber

  def /(that: LynxNumber): LynxNumber

  override def toString: String = number.toString

  def toDouble: Double = this.number.doubleValue()

  def toLynxFloat: LynxFloat = this match{
    case LynxInteger(i) => LynxFloat(i.toDouble)
    case f: LynxFloat => f
  }

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case n: LynxNumber => toDouble.compareTo(n.toDouble)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
