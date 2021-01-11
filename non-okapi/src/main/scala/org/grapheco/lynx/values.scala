package org.grapheco.lynx

import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTInteger, CTNode, CTRelationship, CTString, CypherType}

trait LynxValue {
  def value: Any

  def cypherType: CypherType
}

case class LynxInteger(v: Int) extends LynxValue {
  def value = v

  def cypherType = CTInteger
}

case class LynxString(v: String) extends LynxValue {
  def value = v

  def cypherType = CTString
}

case class LynxBoolean(v: Boolean) extends LynxValue {
  def value = v

  def cypherType = CTBoolean
}

object LynxNull extends LynxValue {
  override def value: Any = null

  override def cypherType: CypherType = CTAny
}

object LynxValue {
  def apply(unknown: Any): LynxValue = {
    unknown match {
      case null => LynxNull
      case v: Boolean => LynxBoolean(v)
      case v: Int => LynxInteger(v)
      case v: Long => LynxInteger(v.toInt)
      case v: String => LynxString(v)
      case v: LynxValue => v
      case _ => throw new InvalidValueException(unknown)
    }
  }
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

class InvalidValueException(unknown: Any) extends LynxException