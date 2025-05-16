package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.runner.{NodeFilter, PropOp}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Range, Expression, NodePattern, RelationshipPattern, SemanticDirection}

import scala.collection.mutable
import scala.language.implicitConversions

case class GraphPatternMatch(val graphPattern: GraphPattern) extends LeafLogicalPlan

sealed abstract class Direction
object Direction{
  def fromCypher(direction: SemanticDirection): Direction = direction match {
    case SemanticDirection.BOTH => BOTH
    case SemanticDirection.INCOMING => IN
    case SemanticDirection.OUTGOING => OUT
  }
}
object IN extends Direction
object OUT extends Direction
object BOTH extends Direction


case class GraphPatternNode(variableName: Option[String],
                            labels: Seq[LynxNodeLabel],
                            properties: Option[Expression],
                            optional: Boolean = false) {
  def withVariableName(newName: Option[String]): GraphPatternNode = this.copy(variableName = newName)
  def withLabels(newLabels: Seq[LynxNodeLabel]): GraphPatternNode = this.copy(labels = newLabels)
  def withProperties(newProperties: Expression): GraphPatternNode = this.copy(properties = Some(newProperties))
}

case class GraphPatternEdge(variableName: Option[String],
                            types: Seq[LynxRelationshipType],
                            properties: Option[Expression],
                            direction: Direction,
                            length: (Int, Int),
                            optional: Boolean = false) {
  def withVariableName(newName: Option[String]): GraphPatternEdge = this.copy(variableName = newName)
}

object ASTConvertor{

  implicit def range(l: Option[Option[Range]]): (Int, Int) = l match {
    case None => (1, 1)
    case Some(None) => (1, Int.MaxValue)
    case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
  }

  implicit def convertNodePattern(pattern: NodePattern): GraphPatternNode =
    GraphPatternNode(pattern.variable.map(_.name),
      pattern.labels.map(_.name).map(LynxNodeLabel),
      pattern.properties)

  implicit def convertEdgePattern(pattern: RelationshipPattern): GraphPatternEdge =
    GraphPatternEdge(pattern.variable.map(_.name),
      pattern.types.map(_.name).map(LynxRelationshipType),
      pattern.properties, Direction.fromCypher(pattern.direction), pattern.length)
}

class GraphPattern {

  private val adjacencyList: mutable.Map[GraphPatternNode, mutable.Set[(GraphPatternEdge, GraphPatternNode)]] = mutable.Map()

  def addNode(node: GraphPatternNode): GraphPatternNode = {
    adjacencyList.getOrElseUpdate(node, mutable.Set())
    node
  }

  def addEdge(source: GraphPatternNode, edge: GraphPatternEdge, target: GraphPatternNode): Unit = {
    val s = addNode(source)
    val t = addNode(target)
    adjacencyList(s).add(edge, t)
  }

  def getNodeByVariableName(variableName: String): Option[GraphPatternNode] =
    adjacencyList.keys.find(_.variableName.forall(variableName.equals))

  def getEdgeByVariableName(variableName: String): Option[GraphPatternEdge] =
    adjacencyList.values.flatten.map(_._1).find(_.variableName.forall(variableName.equals))

  def edgesOf(node: GraphPatternNode): Set[GraphPatternEdge] =
    adjacencyList.getOrElse(node, Set()).map(_._1).toSet

  def neighbors(node: GraphPatternNode): Set[GraphPatternNode] =
    adjacencyList.getOrElse(node, Set()).map(_._2).toSet

  def allNodes: Seq[GraphPatternNode] = adjacencyList.keys.toSeq

}
