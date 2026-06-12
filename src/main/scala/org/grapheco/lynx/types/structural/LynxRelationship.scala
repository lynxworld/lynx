package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.traits.HasProperty
import org.grapheco.lynx.types.{LTRelationship, LynxValue, RelationshipType, TypeMismatchException}

trait LynxRelationship extends LynxElement {
  val startNodeId: LynxId
  val endNodeId: LynxId

  def value: LynxRelationship = this

  def relationType: Option[LynxRelationshipType]

  def lynxType: RelationshipType = LTRelationship

  override def sameTypeCompareTo(o: LynxValue): Int = o match {
    case r: LynxRelationship => this.id.toLynxInteger.compareTo(r.id.toLynxInteger)
    case _ => throw TypeMismatchException(this.lynxType, o.lynxType)
  }
}
