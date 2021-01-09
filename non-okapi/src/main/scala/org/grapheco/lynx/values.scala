package org.grapheco.lynx

import org.opencypher.v9_0.util.symbols.{CTInteger, CTNode, CTRelationship, CTString, CypherType}

trait CypherValue {
  def value: Any

  def cypherType: CypherType
}

case class CypherInteger(v: Int) extends CypherValue {
  def value = v

  def cypherType = CTInteger
}

case class CypherString(v: String) extends CypherValue {
  def value = v

  def cypherType = CTString
}

object CypherValue {
  def apply(unknown: Any): CypherValue = {
    unknown match {
      case v: Int => CypherInteger(v)
      case v: Long => CypherInteger(v.toInt)
      case v: String => CypherString(v)
      case v: CypherValue => v
      case _ => throw new UnrecognizedCypherValueException(unknown)
    }
  }
}

trait CypherId {
  val value: Any
}

trait CypherNode extends CypherValue {
  val id: CypherId

  def value = this

  def cypherType = CTNode
}

trait CypherRelationship extends CypherValue {
  val id: CypherId
  val startNodeId: CypherId
  val endNodeId: CypherId

  def value = this

  def cypherType = CTRelationship
}

class UnrecognizedCypherValueException(unknown: Any) extends LynxException