package org.grapheco.lynx.types

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.time._
import org.opencypher.v9_0.expressions.{BooleanLiteral, DoubleLiteral, IntegerLiteral, StringLiteral}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTInteger, CTString, CypherType}

import java.time._
import scala.collection.mutable

trait TypeSystem {
  def typeOf(clazz: Class[_]): LynxType

  def wrap(value: Any): LynxValue
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
    case v: Map[String, Any] => LynxMap(v.mapValues(wrap))
    case v: Array[Int] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Long] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Double] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Float] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Boolean] => LynxList(v.map(wrap(_)).toList)
    case v: Array[String] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Any] => LynxList(v.map(wrap(_)).toList)
    case v: Iterable[Any] => LynxList(v.map(wrap(_)).toList)
    case _ => throw InvalidValueException(value)
  }
}