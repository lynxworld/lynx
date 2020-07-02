package org.opencypher.lynx

import java.util.concurrent.atomic.AtomicLong

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr.Expr

import scala.collection.mutable

/**
 * Created by bluejoe on 2020/4/28.
 */
case class LynxNode(id: Long, labels: Set[String], properties: CypherMap) extends Node[Long] {
  override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode =
    LynxNode(id: Long, labels: Set[String], properties)

  override type I = LynxNode
}

case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, properties: CypherMap) extends Relationship[Long] {
  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): LynxRelationship.this.type = ???

  override type I = this.type
}

class InMemoryPropertyGraph extends LynxPropertyGraph with Logging {
  val nodes = mutable.Map[Long, LynxNode]();
  val rels = mutable.Map[Long, LynxRelationship]();
  val nodeId = new AtomicLong(1)
  val relId = new AtomicLong(1)

  def filterNodesApproximately(expr: Expr, labels: Set[String], parameters: CypherMap): Stream[LynxNode] = {
    logger.debug(s"filterNodesApproximately: $expr")
    scanNodes(labels, true)
  }

  def addNode(labels: Set[String], prop: (String, Any)*) = {
    val node = LynxNode(nodeId.getAndIncrement(), labels, CypherMap(prop: _*))
    nodes += node.id -> node
    node
  }

  def addRelationship(srcNodeId: Long, targetNodeId: Long, relName: String, prop: (String, Any)*) = {
    val rel = LynxRelationship(relId.getAndIncrement(), srcNodeId, targetNodeId, relName, CypherMap(prop: _*))
    rels += rel.id -> rel
    rel
  }

  def scanNodes(labels: Set[String], exactLabelMatch: Boolean): Stream[LynxNode] =
    labels match {
      case _ if labels.isEmpty => nodes.values.toStream
      case _ if (exactLabelMatch) =>
        nodes.values.filter(x => x.labels.eq(labels)).toStream
      case _ =>
        nodes.values.filter(x => x.labels.find(y => labels.contains(y)).isDefined).toStream
    }

  def scanRelationships(relCypherTypeOption: Option[String]): Stream[LynxRelationship] =
    relCypherTypeOption.map(relCypherType => rels.values.filter(_.relType.equals(relCypherType))).getOrElse(rels.values).toStream

  override def node(id: Long): LynxNode = nodes(id)

  override def relationship(id: Long): LynxRelationship = rels(id)
}



abstract class LynxPropertyGraph extends PropertyGraph {
  def node(id: Long): LynxNode

  def relationship(id: Long): LynxRelationship

  def scanNodes(labels: Set[String], exactLabelMatch: Boolean): Stream[LynxNode]

  def scanRelationships(relCypherType: Option[String]): Stream[LynxRelationship]

  def filterNodesApproximately(expr: Expr, labels: Set[String], parameters: CypherMap): Stream[LynxNode]

  def filterNodes(fields: Set[IRField], predicates: Set[Expr], labels: Set[String], parameters: CypherMap): LynxCypherRecords = {
    val nodes = filterNodesApproximately(predicates.head, labels, parameters)
    //2nd filtering
    val rec = LynxCypherRecords.nodes(fields.head.name, nodes)
    rec.filter(predicates.head, parameters)
  }

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxCypherRecords =
    LynxCypherRecords.nodes(name, scanNodes(nodeCypherType.labels, exactLabelMatch))

  override def relationships(name: String, relCypherType: CTRelationship): LynxCypherRecords =
    LynxCypherRecords.rels(name, scanRelationships(Some(relCypherType.name)))

  var _session: CypherSession = null

  def setSession(session: CypherSession) = _session = session

  override def session: CypherSession = _session

  override def unionAll(others: PropertyGraph*): PropertyGraph = ???

  override def schema: PropertyGraphSchema = PropertyGraphSchema.empty.withNodePropertyKeys("person")("name" -> CTString, "age" -> CTInteger)
}

