package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTRelationship, RelationshipType}

trait LynxRelationship extends LynxValue with HasProperty {
  val id: LynxId
  val startNodeId: LynxId
  val endNodeId: LynxId

  def value: LynxRelationship = this

  def relationType: Option[LynxRelationshipType]

  def cypherType: RelationshipType = CTRelationship
}
