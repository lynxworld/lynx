package org.grapheco.lynx.logical.plans

import org.grapheco.lynx.runner.{NodeFilter, PropOp}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LogicalVariable, NodePattern, Range, RelationshipPattern, SemanticDirection, VirtualPattern}

import scala.collection.mutable
import scala.language.implicitConversions

case class GraphPatternMatch(graphPattern: GraphPattern, filters: FilterExpression = FilterExpression()) extends LeafLogicalPlan

sealed abstract class Direction
object Direction{
  def fromCypher(direction: SemanticDirection): Direction = direction match {
    case SemanticDirection.BOTH => BOTH
    case SemanticDirection.INCOMING => IN
    case SemanticDirection.OUTGOING => OUT
  }

  def toCypher(direction: Direction): SemanticDirection = direction match {
    case IN => SemanticDirection.INCOMING
    case OUT => SemanticDirection.OUTGOING
    case BOTH => SemanticDirection.BOTH
  }

  def reverse(direction: Direction): Direction = direction match {
    case IN => OUT
    case OUT => IN
    case BOTH => BOTH
  }
}
object IN extends Direction
object OUT extends Direction
object BOTH extends Direction

object FilterExpression {
  val empty: FilterExpression = FilterExpression()
}
case class FilterExpression(filters: Map[Set[String], Seq[Expression]] = Map.empty) {

  def filtersByNames(nodeNames: Set[String]): Seq[Expression] = {
    filters.getOrElse(nodeNames, Seq())
  }

  def filtersCoverIn(nodeNames: Set[String]): Seq[Expression] = {
    filters.filterKeys(_.subsetOf(nodeNames)).values.flatten.toSeq
  }

  def filtersInvolvedIn(nodeNames: Set[String]): Seq[Expression] = {
    filters.filterKeys(_.intersect(nodeNames).nonEmpty).values.flatten.toSeq
  }

  def addFilter(nodeNames: Set[String], filter: Expression): FilterExpression = {
    val newFilters = filters.getOrElse(nodeNames, Seq()) :+ filter
    this.copy(filters = filters + (nodeNames -> newFilters))
  }

  def combine(other: FilterExpression): FilterExpression = {
    if (this.filters.isEmpty) return other
    if (other.filters.isEmpty) return this
    val combinedFilters = this.filters ++ other.filters.map { case (nodeNames, filters) =>
      val existingFilters = filters ++ this.filters.getOrElse(nodeNames, Seq())
      (nodeNames, existingFilters)
    }
    this.copy(filters = combinedFilters)
  }

  def removeFilter(nodeNames: Set[String], filter: Expression): FilterExpression = {
    val existingFilters = filters.getOrElse(nodeNames, Seq())
    val updatedFilters = existingFilters.filterNot(_ == filter)
    this.copy(filters = filters + (nodeNames -> updatedFilters))
  }

}

trait GraphPatternElement {
  def variableName: String
  def expressions: Seq[Expression]
  def optional: Boolean
  def virtual: Boolean
  def withExpressions(expressions: Seq[Expression]): GraphPatternElement
  def addExpressions(newProperties: Seq[Expression]): GraphPatternElement
  def propertyStr: String = if (expressions.nonEmpty) expressions.map(_.toString).mkString("{",",","}") else ""
}

case class GraphPatternNode(variableName: String,
                            labels: Seq[LynxNodeLabel],
                            expressions: Seq[Expression],
                            virtual: Boolean = false,
                            optional: Boolean = false) extends GraphPatternElement {
//  def withVariableName(newName: String): GraphPatternNode = this.copy(variableName = newName)
  def withLabels(newLabels: Seq[LynxNodeLabel]): GraphPatternNode = this.copy(labels = newLabels)
  def addLabels(labels: Seq[LynxNodeLabel]): GraphPatternNode = this.copy(labels = this.labels ++ labels)
  def withExpressions(expressions: Seq[Expression]): GraphPatternElement = this.copy(expressions = expressions)
  def addExpressions(newProperties: Seq[Expression]): GraphPatternNode = this.copy(expressions = expressions ++ newProperties)
  override def toString: String = {
    val labelStr = labels.map(_.value).mkString(":")
    s"($variableName:$labelStr${propertyStr})"
  }
}

case class GraphPatternEdge(variableName: String,
                            types: Seq[LynxRelationshipType],
                            expressions: Seq[Expression],
                            direction: Direction,
                            length: (Int, Int),
                            virtual: Boolean = false,
                            optional: Boolean = false) extends GraphPatternElement {
//  def withVariableName(newName: String): GraphPatternEdge = this.copy(variableName = newName)
  def reversed: GraphPatternEdge = this.copy(direction = Direction.reverse(direction))
  def withExpressions(expressions: Seq[Expression]): GraphPatternElement = this.copy(expressions = expressions)
  def addExpressions(newProperties: Seq[Expression]): GraphPatternEdge = this.copy(expressions = expressions ++ newProperties)
  override def toString: String = {
    val typeStr = types.map(_.value).mkString(":")
    val body = s"[$variableName:$typeStr${propertyStr}]"
    direction match {
      case IN => "<-" + body + "-"
      case OUT => "-" + body + "->"
      case BOTH => "-" + body + "-"
    }
  }
}

object ASTConvertor{

  implicit def range(l: Option[Option[Range]]): (Int, Int) = l match {
    case None => (1, 1)
    case Some(None) => (1, Int.MaxValue)
    case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
  }

  implicit def convertNodePattern(pattern: NodePattern): GraphPatternNode =
    GraphPatternNode(pattern.variable.map(_.name).getOrElse(s"_node${pattern.hashCode}"),
      pattern.labels.map(LynxNodeLabel.fromNodeLabel),
      pattern.properties.toSeq,
      virtual = pattern.isInstanceOf[VirtualPattern])

  implicit def convertEdgePattern(pattern: RelationshipPattern): GraphPatternEdge =
    GraphPatternEdge(pattern.variable.map(_.name).getOrElse(s"_edge${pattern.hashCode}"),
      pattern.types.map(_.name).map(LynxRelationshipType),
      pattern.properties.toSeq, Direction.fromCypher(pattern.direction),
      pattern.length,
      virtual = pattern.isInstanceOf[VirtualPattern])
}

class GraphPattern {
  // 存储所有节点，key为variableName或hashcode
  private val nodesMap: mutable.Map[String, GraphPatternNode] = mutable.Map()

  // 存储所有边，key为variableName或hashcode
  private val edgesMap: mutable.Map[String, GraphPatternEdge] = mutable.Map()

  private val relMap: mutable.Map[String, (String, String)] = mutable.Map()

  private case class RelNode(rel: String, node: String, reverse: Boolean = false)

  // 邻接表使用节点的key作为索引
  private val adjacencyList: mutable.Map[String, mutable.Set[RelNode]] = mutable.Map()

  private val bfs: mutable.Map[String, mutable.Set[RelNode]] = mutable.Map()

  def getKey(element: GraphPatternElement): String = element match {
    case node: GraphPatternNode => node.variableName
    case edge: GraphPatternEdge => edge.variableName
  }

  // 添加节点，返回节点的key
  def addNode(node: GraphPatternNode): String = {
    val key = getKey(node)
    if (!nodesMap.contains(key)) {
      nodesMap.put(key, node)
      adjacencyList.getOrElseUpdate(key, mutable.Set())
    }
    key
  }

  // 添加边
  def addEdge(source: GraphPatternNode, edge: GraphPatternEdge, target: GraphPatternNode): Unit = {
    val sourceKey = addNode(source)
    val targetKey = addNode(target)
    val edgeKey = getKey(edge)

    edgesMap.put(edgeKey, edge)
    relMap.put(edgeKey, (sourceKey, targetKey))
    adjacencyList(sourceKey).add(RelNode(edgeKey, targetKey))
    adjacencyList(targetKey).add(RelNode(edgeKey, sourceKey, reverse = true))
    bfs.getOrElseUpdate(sourceKey, mutable.Set()).add(RelNode(edgeKey, targetKey))
  }

  // 更新节点
  def updateNode(node: GraphPatternNode): Unit = {
    val key = getKey(node)
    if (nodesMap.contains(key)) {
      nodesMap.update(key, node)
    }
  }

  // 更新边
  def updateEdge(edge: GraphPatternEdge): Unit = {
    val key = getKey(edge)
    if (edgesMap.contains(key)) {
      edgesMap.update(key, edge)
    }
  }

  // 根据variableName查找节点
  def maybeNode(variableName: String): Option[GraphPatternNode] = nodesMap.get(variableName)

  // 根据variableName查找边
  def maybeEdge(variableName: String): Option[GraphPatternEdge] = edgesMap.get(variableName)

  private def adjacency(node: GraphPatternNode): Set[RelNode] = {
    adjacencyList.getOrElse(node.variableName, Set()).toSet
  }

  def edgesOf(node: GraphPatternNode, reversed: Boolean = false): Set[GraphPatternEdge] =
    adjacency(node).filter(_.reverse==reversed).map(_.rel).flatMap(maybeEdge)
  // 获取节点的所有邻居节点
  def neighbors(node: GraphPatternNode): Set[GraphPatternNode] = adjacency(node).map(_.node).flatMap(maybeNode)

  // 获取所有节点
  def allNodes: Seq[GraphPatternNode] = nodesMap.values.toList

  def nodesOf(edge: GraphPatternEdge): (GraphPatternNode, GraphPatternNode) = {
    val (sourceKey, targetKey) = relMap(edge.variableName)
    (maybeNode(sourceKey).get, maybeNode(targetKey).get)
  }


  // 检查是否包含指定variableName的元素
  def containsElement(variableName: String): Boolean =
    maybeNode(variableName).isDefined || maybeEdge(variableName).isDefined

  // 根据variableName获取元素
  def getElementByVariableName(variableName: String): Option[GraphPatternElement] =
    maybeNode(variableName).orElse(maybeEdge(variableName))

  // 生成指定大小的所有连通子图
  def generateConnectedSubsets(size: Int): Set[Set[GraphPatternNode]] = {
    val allNodes = this.allNodes.map(_.variableName).toSet
    val result = mutable.Set[Set[String]]()

    // 从每个节点开始，进行BFS扩展到指定大小
    for (startNode <- allNodes) {
      val visited = mutable.Set[String]()
      val queue = mutable.Queue[String]()
      val stack = mutable.Stack[String]()

      queue.enqueue(startNode)
      visited.add(startNode)

      while (queue.nonEmpty && visited.size < size) {
        val current = queue.dequeue()

        // 获取当前节点的所有邻居
//        val neighbors = stack.headOption match {
//          case Some(top) => this.neighbors_dfs(current).filterNot(top.eq)
//          case None => this.neighbors_dfs(current)
//        }
        val neighbors = this.bfs.getOrElse(current, Set.empty).map(_.node)
        for (neighbor <- neighbors if !visited.contains(neighbor)) {
          visited.add(neighbor)
          queue.enqueue(neighbor)

          if (visited.size == size) {
            // 找到一个大小为size的连通子图
            result.add(visited.toSet)
            // 重置访问状态，继续寻找其他可能的连通子图
            visited.remove(neighbor)
          }
        }
        stack.push(current)
      }

      // 如果恰好找到一个大小为size的连通子图
      if (visited.size == size) {
        result.add(visited.toSet)
      }
    }

    result.map(subset => subset.map(maybeNode(_).get)).toSet
  }

  // 找到连接两个子图的边
  def findConnectingEdges(left: Set[GraphPatternNode], right: Set[GraphPatternNode]): Set[GraphPatternEdge] = {
    val leftVariableNames = left.map(_.variableName)
    val rightVariableNames = right.map(_.variableName)
    relMap.collect{
      case (edgeKey, (sourceKey, targetKey)) if leftVariableNames.contains(sourceKey) && rightVariableNames.contains(targetKey) => maybeEdge(edgeKey).get
      case (edgeKey, (sourceKey, targetKey)) if leftVariableNames.contains(targetKey) && rightVariableNames.contains(sourceKey) => maybeEdge(edgeKey).get
    }.toSet
  }
}
