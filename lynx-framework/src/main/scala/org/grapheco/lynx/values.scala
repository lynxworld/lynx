package org.grapheco.lynx

import java.util.Date

import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTFloat, CTInteger, CTList, CTMap, CTNode, CTRelationship, CTString, CypherType}

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
  def value = v

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

  def cypherType = CTInteger
}

case class LynxDouble(v: Double) extends LynxNumber {
  def value = v

  def number: Number = v

  def cypherType = CTFloat

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
  def value = v

  def cypherType = CTString

  override def >(lynxValue: LynxValue): Boolean = this.value > lynxValue.asInstanceOf[LynxString].value

  override def >=(lynxValue: LynxValue): Boolean = this.value >= lynxValue.asInstanceOf[LynxString].value

  override def <(lynxValue: LynxValue): Boolean = this.value < lynxValue.asInstanceOf[LynxString].value

  override def <=(lynxValue: LynxValue): Boolean = this.value <= lynxValue.asInstanceOf[LynxString].value
}

case class LynxBoolean(v: Boolean) extends LynxValue {
  def value = v

  def cypherType = CTBoolean
}

trait LynxCompositeValue extends LynxValue

case class LynxList(v: List[LynxValue]) extends LynxCompositeValue {
  override def value = v

  override def cypherType: CypherType = CTList(CTAny)
}

case class LynxMap(v: Map[String, LynxValue]) extends LynxCompositeValue {
  override def value = v

  override def cypherType: CypherType = CTMap
}

trait LynxTemporalValue extends LynxValue

case class LynxDate(ms: Long) extends LynxTemporalValue {
  def value = ms

  def cypherType = CTDate
}

case class LynxDateTime(ms: Long) extends LynxTemporalValue {
  def value = ms

  def cypherType = CTDateTime
}

object LynxNull extends LynxValue {
  override def value: Any = null

  override def cypherType: CypherType = CTAny
}

trait LynxId {
  val value: Any
}

trait LynxNode extends LynxValue {
  val id: LynxId

  def value = this

  def labels: Seq[String]

  def property(name: String): Option[LynxValue]

  def cypherType = CTNode
}

trait LynxRelationship extends LynxValue {
  val id: LynxId
  val startNodeId: LynxId
  val endNodeId: LynxId

  def value = this

  def relationType: Option[String]

  def property(name: String): Option[LynxValue]

  def cypherType = CTRelationship
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
    case v: Date => LynxDate(v.getTime)
    case v: Iterable[Any] => LynxList(v.map(LynxValue(_)).toList)
    case v: Map[String, Any] => LynxMap(v.map(x => x._1 -> LynxValue(x._2)))
    case v: Array[Any] => LynxList(v.map(LynxValue(_)).toList)
    case _ => throw InvalidValueException(value)
  }
}

case class InvalidValueException(unknown: Any) extends LynxException