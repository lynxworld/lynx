package org.grapheco.lynx

trait CypherValue {
  def value: Any
}

case class CypherInteger(v: Int) extends CypherValue {
  def value = v
}
