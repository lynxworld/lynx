package org.grapheco.lynx.types

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.types.time.{LynxDate, LynxDateTime, LynxLocalDateTime, LynxLocalTime, LynxTime}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

/**
 * @ClassName LynxValue
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
trait LynxValue extends Comparable[LynxValue]{
  def value: Any

  def lynxType: LynxType

  // TODO: Impl the Compare func for LynxValue
  def >(lynxValue: LynxValue): Boolean = this.compareTo(lynxValue) > 0

  def >=(lynxValue: LynxValue): Boolean = this.compareTo(lynxValue) >= 0

  def <(lynxValue: LynxValue): Boolean = this.compareTo(lynxValue) < 0

  def <=(lynxValue: LynxValue): Boolean = this.compareTo(lynxValue) <= 0

  def sameTypeCompareTo(o: LynxValue): Int

  /*
    To accomplish this, we propose a pre-determined order of types and ensure that each value falls
    under exactly one disjoint type in this order.
     */
  private final val Map = 1
  private final val NODE = 2
  private final val RELATIONSHIP = 3
  private final val LIST = 4
  private final val PATH = 5
  private final val STRING = 6
  private final val BOOLEAN = 7
  private final val NUMBER = 8
  private final val VOID = 9

  def typeOrder(lynxValue: LynxValue): Int = lynxValue match {
    case _: LynxMap => Map
    case _: LynxNode => NODE
    case _: LynxRelationship => RELATIONSHIP
    case _: LynxList => LIST
    //      case _: path todo
    case _: LynxString => STRING
    case _: LynxBoolean => BOOLEAN
    case _: LynxNumber => NUMBER
    case LynxNull => VOID
    case _ => 0
  }

//  val ordering: Ordering[LynxValue] = (x: LynxValue, y: LynxValue) => {
//    val o1 = LynxValue.typeOrder(x)
//    val o2 = LynxValue.typeOrder(y)
//    if (o1 == o2) x.compareTo(y)
//    else o1 - o2
//  }

  override def compareTo(o: LynxValue): Int = {
    val o1 = typeOrder(this)
    val o2 = typeOrder(o)
    if (o1 == o2) this.sameTypeCompareTo(o)
    else o1 - o2
  }

}

object LynxValue {
  def apply(value: Any): LynxValue = value match {
    case null => LynxNull
    case v: LynxValue => v
    case v: Boolean => LynxBoolean(v)
    case v: Int => LynxInteger(v)
    case v: Long => LynxInteger(v)
    case v: String => LynxString(v)
    case v: Double => LynxFloat(v)
    case v: Float => LynxFloat(v)
    case v: LocalDate => LynxDate(v)
    case v: ZonedDateTime => LynxDateTime(v)
    case v: LocalDateTime => LynxLocalDateTime(v)
    case v: LocalTime => LynxLocalTime(v)
    case v: OffsetTime => LynxTime(v)
    case v: Iterable[Any] => LynxList(v.map(apply(_)).toList)
    case v: Map[String, Any] => LynxMap(v.map(x => x._1 -> apply(x._2)))
    case v: Array[Int] => LynxList(v.map(apply(_)).toList)
    case v: Array[Long] => LynxList(v.map(apply(_)).toList)
    case v: Array[Double] => LynxList(v.map(apply(_)).toList)
    case v: Array[Float] => LynxList(v.map(apply(_)).toList)
    case v: Array[Boolean] => LynxList(v.map(apply(_)).toList)
    case v: Array[String] => LynxList(v.map(apply(_)).toList)
    case v: Array[Any] => LynxList(v.map(apply(_)).toList)
    case _ => throw InvalidValueException(value)
  }


}