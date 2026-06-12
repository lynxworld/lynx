package org.grapheco.lynx.infer.cache.core

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}

import scala.collection.mutable

object Meta {
  val mapper: mutable.Map[Int, String] = mutable.Map(
    "$LABEL".hashCode -> "$LABEL",
    "$ALIVE".hashCode -> "$ALIVE",
    "$PROPS".hashCode -> "$PROPS"
  )

  val LABEL: Int = "$LABEL".hashCode

  val ALIVE: Int = "$ALIVE".hashCode

  val PROPS: Int = "$PROPS".hashCode

  def getCode(pk: LynxPropertyKey): Int = {
    mapper.put(pk.value.hashCode, pk.value)
    pk.value.hashCode
  }

  def getCode(label: LynxNodeLabel): Int = {
    mapper.put(label.value.hashCode, label.value)
    label.value.hashCode
  }

  def getCode(relType: LynxRelationshipType): Int = {
    mapper.put(relType.value.hashCode, relType.value)
    relType.value.hashCode
  }

  val NO_EDGE: LynxRelationship = new LynxRelationship {
    val ghost: LynxId = new LynxId {
      override val value: Any = -1

      override def toLynxInteger: LynxInteger = LynxInteger(-1)
    }
    override val startNodeId: LynxId = ghost
    override val endNodeId: LynxId = ghost

    override def relationType: Option[LynxRelationshipType] = None

    override val id: LynxId = ghost

    override def keys: Seq[LynxPropertyKey] = Seq.empty

    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = None
  }
}
