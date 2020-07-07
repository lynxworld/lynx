package cn.pandadb.lynx

import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, Relationship}

object LynxNode {
  def apply(id: Long, props: (String, Any)*) = new LynxNode(id, Set.empty, props.map(kv => kv._1 -> CypherValue(kv._2)): _*)
}

case class LynxNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] with LynxElement {
  val properties = props.toMap
  val withIds = props.toMap + ("_id" -> CypherValue(id))
  override type I = this.type

  override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode.this.type = this
}

case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends Relationship[Long] with LynxElement {
  val properties = props.toMap
  val withIds = props.toMap ++ Map("_id" -> CypherValue(id), "_from" -> CypherValue(startId), "_to" -> CypherValue(endId))
  override type I = this.type

  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): LynxRelationship.this.type = this
}

trait LynxElement {

}