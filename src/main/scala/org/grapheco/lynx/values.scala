package org.grapheco.lynx

import org.grapheco.lynx

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import org.opencypher.v9_0.util.symbols.{BooleanType, CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTInteger, CTList, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTRelationship, CTString, CTTime, CypherType, DateTimeType, DateType, FloatType, IntegerType, NodeType, RelationshipType, StringType}

import scala.language.implicitConversions

trait LynxValue {
  def value: Any

  def cypherType: LynxType

  def >(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def >=(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def <(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)

  def <=(lynxValue: LynxValue): Boolean = this.value.equals(lynxValue.value)
}

trait LynxNumber extends LynxValue {
  def number: Number

  def +(that: LynxNumber): LynxNumber

  def -(that: LynxNumber): LynxNumber

}

case class LynxInteger(v: Long) extends LynxNumber {
  def value: Long = v

  def number: Number = v

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxInteger(v + v2)
      case LynxDouble(v2) => LynxDouble(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxInteger(v - v2)
      case LynxDouble(v2) => LynxDouble(v - v2)
    }
  }

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxInteger].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxInteger].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxInteger].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxInteger].value

  def cypherType: IntegerType = CTInteger
}

case class LynxDouble(v: Double) extends LynxNumber {
  def value: Double = v

  def number: Number = v

  def cypherType: FloatType = CTFloat

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxDouble(v + v2)
      case LynxDouble(v2) => LynxDouble(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxDouble(v - v2)
      case LynxDouble(v2) => LynxDouble(v - v2)
    }
  }

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxDouble].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxDouble].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxDouble].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxDouble].value
}

case class LynxString(v: String) extends LynxValue {
  def value: String = v

  def cypherType: StringType = CTString

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxString].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxString].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxString].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxString].value
}

case class LynxBoolean(v: Boolean) extends LynxValue {
  def value: Boolean = v

  def cypherType: BooleanType = CTBoolean
}

trait LynxCompositeValue extends LynxValue

case class LynxList(v: List[LynxValue]) extends LynxCompositeValue {
  override def value: List[LynxValue] = v

  override def cypherType: CypherType = CTList(CTAny)
}

case class LynxMap(v: Map[String, LynxValue]) extends LynxCompositeValue {
  override def value: Map[String, LynxValue] = v

  override def cypherType: CypherType = CTMap
}

trait LynxTemporalValue extends LynxValue

case class LynxDate(localDate: LocalDate) extends LynxTemporalValue {
  def value: LocalDate = localDate
  def cypherType: DateType = CTDate
}

case class LynxDateTime(zonedDateTime: ZonedDateTime) extends LynxTemporalValue {
  def value: ZonedDateTime = zonedDateTime

  def cypherType: DateTimeType = CTDateTime
}

case class LynxLocalDateTime(localDateTime: LocalDateTime) extends LynxTemporalValue {
  def value: LocalDateTime = localDateTime

  def cypherType: LynxType = CTLocalDateTime
}

case class LynxLocalTime(localTime: LocalTime) extends LynxTemporalValue {
  def value: LocalTime = localTime

  def cypherType: LynxType = CTLocalTime
}

case class LynxTime(offsetTime: OffsetTime) extends LynxTemporalValue {
  def value: OffsetTime = offsetTime

  def cypherType: LynxType = CTTime
}

case class LynxDuration(duration: Duration) extends LynxTemporalValue {
  def value: Duration = duration

  def cypherType: LynxType = CTDuration
}

object LynxNull extends LynxValue {
  override def value: Any = null

  override def cypherType: CypherType = CTAny
}

trait LynxId {
  val value: Any
}
object NameParser{
  implicit def parseLabelName(name: String): LynxNodeLabel = LynxNodeLabel(name)
  implicit def parseTypeName(name: String): LynxRelationshipType = LynxRelationshipType(name)
  implicit def parseKeyName(name: String): LynxPropertyKey = LynxPropertyKey(name)
}
case class LynxNodeLabel(value: String)

case class LynxRelationshipType(value: String)

case class LynxPropertyKey(value: String)

trait HasProperty {
  def property(propertyKey: LynxPropertyKey): Option[LynxValue]
}

trait LynxNode extends LynxValue with HasProperty {
  val id: LynxId

  def value: LynxNode = this

  def labels: Seq[LynxNodeLabel]

  def cypherType: NodeType = CTNode
}

trait LynxRelationship extends LynxValue  with HasProperty {
  val id: LynxId
  val startNodeId: LynxId
  val endNodeId: LynxId

  def value: LynxRelationship = this

  def relationType: Option[LynxRelationshipType]

  def cypherType: RelationshipType = CTRelationship
}

object LynxValue {
  def apply(value: Any): LynxValue = value match {
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

case class InvalidValueException(unknown: Any) extends LynxException