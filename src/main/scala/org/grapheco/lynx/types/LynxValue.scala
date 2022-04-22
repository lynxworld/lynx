package org.grapheco.lynx.types

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.time.{LynxDate, LynxDateTime, LynxLocalDateTime, LynxLocalTime, LynxTime}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

/**
 * @ClassName LynxValue
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
trait LynxValue {
  def value: Any

  def lynxType: LynxType

  def >(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def >=(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def <(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def <=(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

//  override def toString: String = "a"
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