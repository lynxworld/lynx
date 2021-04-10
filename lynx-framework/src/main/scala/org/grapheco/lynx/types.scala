package org.grapheco.lynx

import java.util.Date

import org.opencypher.v9_0.expressions.{BooleanLiteral, CountStar, DoubleLiteral, FunctionInvocation, IntegerLiteral, Parameter, StringLiteral, Variable}
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

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
    case v: Long => LynxInteger(v.toInt)
    case v: String => LynxString(v)
    case v: Double => LynxDouble(v)
    case v: Float => LynxDouble(v)
    case v: Date => LynxDate(v.getTime)
    case v: Iterable[Any] => LynxList(v.map(wrap(_)).toList)
    case v: Map[String, Any] => LynxMap(v.map(x => x._1 -> wrap(x._2)))
    case v: Array[Any] => LynxList(v.map(wrap(_)).toList)
    case _ => throw InvalidValueException(value)
  }
}

