package org.grapheco.lynx

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import org.opencypher.v9_0.expressions.{BooleanLiteral, CountStar, DoubleLiteral, FunctionInvocation, IntegerLiteral, Parameter, StringLiteral, Variable}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

import scala.collection.mutable

trait TypeSystem {
  def typeOf(clazz: Class[_]): LynxType

  def wrap(value: Any): LynxValue

  def unwrap(value: Any): Any = value match {
    case lv: LynxValue => lv.value
    case _ => value
  }
}

class DefaultTypeSystem extends TypeSystem {
  val mapTypes: mutable.Map[Class[_], LynxType] = mutable.Map(
    classOf[BooleanLiteral] -> CTBoolean,
    classOf[StringLiteral] -> CTString,
    classOf[IntegerLiteral] -> CTInteger,
    classOf[DoubleLiteral] -> CTInteger
  )

  override def typeOf(clazz: Class[_]): CypherType = mapTypes.getOrElse(clazz, CTAny)

  override def wrap(value: Any): LynxValue = value match {
    case null => LynxNull
    case v: LynxValue => v
    case v: Boolean => LynxBoolean(v)
    case v: Int => LynxInteger(v)
    case v: Long => LynxInteger(v)
    case v: String => LynxString(v)
    case v: Double => LynxDouble(v)
    case v: Float => LynxDouble(v)
    case v: LocalDate => LynxDate(v)
    case v: ZonedDateTime => LynxDateTime(v)
    case v: LocalDateTime => LynxLocalDateTime(v)
    case v: LocalTime => LynxLocalTime(v)
    case v: OffsetTime => LynxTime(v)
    case v: Duration => LynxDuration(v)
    case v: Iterable[Any] => LynxList(v.map(wrap(_)).toList)
    case v: Map[String, Any] => LynxMap(v.map(x => x._1 -> wrap(x._2)))
    case v: Array[Int] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Long] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Double] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Float] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Boolean] => LynxList(v.map(wrap(_)).toList)
    case v: Array[String] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Any] => LynxList(v.map(wrap(_)).toList)
    case _ => throw InvalidValueException(value)
  }
}