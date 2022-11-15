package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.{LynxValue, TypeMismatchException}
import org.opencypher.v9_0.util.symbols.{CTRelationship, RelationshipType}

trait LynxRelationship extends LynxValue with HasProperty with LynxElement {
  val id: LynxId
  val startNodeId: LynxId
  val endNodeId: LynxId

  def value: LynxRelationship = this

  def relationType: Option[LynxRelationshipType]

  def lynxType: RelationshipType = CTRelationship

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case r: LynxRelationship => this.id.toLynxInteger.compareTo(r.id.toLynxInteger)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
