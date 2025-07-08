package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.LynxException
import org.grapheco.lynx.logical.plans.{FilterExpression, GraphPattern, GraphPatternEdge, GraphPatternMatch, GraphPatternNode}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.planner.translators.MetaData._
import org.grapheco.lynx.physical.plans.{Expand, ExpandFactory, Filter, InferExpand, InferFakeNode, InferPhysicalPlan, InferPlanner, InferProperties, NodesPlanFactory, PhysicalPlan, PhysicalPlanBuffer, RelationshipsPlanFactory}
import org.grapheco.lynx.runner.{GraphModel, IndexManager}
import org.opencypher.v9_0.expressions.{Expression, VirtualPattern, VirtualRelationshipPattern}

import scala.collection.mutable

class CostBasedPlanner(costCalculator: CostCalculator) {
  val estimate: Candidate => Candidate = costCalculator.estimate

  // 迭代动态规划算法实现
  def plan(graphPatternMatch: GraphPatternMatch)(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    val graphModel: GraphModel = ppc.runnerContext.graphModel

    val GraphPatternMatch(graph: GraphPattern, filters: FilterExpression) = graphPatternMatch

    // 初始化DP表，用于存储子问题的最优解
    implicit val dpTable: DPTable = new DPTable()

    // 初始化单节点计划, 只保留最优的计划
    graph.allNodes.map{ n =>
      if (n.virtual) n -> Candidate(InferFakeNode(n))
      else n -> DefaultNodePlanner(n).plan
        .map(n => Candidate(n))
        .map(estimate)
        .minBy(_.cost) //TODO top 3
    }.foreach{ case (node, candidate) => dpTable.put(Set(node), candidate)}

    // 迭代构建更大的连通子图
    val allNodes = graph.allNodes.toSet
    val maxSize = allNodes.size

    // 从大小为2的子图开始，逐步构建到包含所有节点的完整图
    for (size <- 2 to maxSize) {
      // 生成所有大小为size的连通子图
      graph.generateConnectedSubsets(size)
//        .filterNot(subset => subset.forall(_.virtual)) // 去掉纯虚图, 留着单个的虚图
        .foreach(findOptimalJoin(graph, _, dpTable, filters)) // 对每个子图，找到最优的连接方式
    }

    // 返回包含所有节点的最优计划
    dpTable(allNodes).plan
  }



  // 为给定子图找到最优的连接方式
  private def findOptimalJoin(graph: GraphPattern, subset: Set[GraphPatternNode],
                             dpTable: DPTable, filters: FilterExpression)(implicit ppc: PhysicalPlannerContext): Unit = {
    var bestPlan: Option[Candidate] = None

    // 枚举所有可能的子图划分
    for (size <- 1 until subset.size) {
      val subsetPartitions = partitionSubset(graph, subset, size)
      // 需要解决的过滤
      val _filters: Seq[Expression] = filters.filtersCoverIn(subset.map(_.variableName))

      for ((left, right) <- subsetPartitions) {
        // 确保两个子集都有最优解
        if (dpTable.contains(left) && dpTable.contains(right)) {
          val leftPlan = dpTable(left)
          val rightPlan = dpTable(right)

          val pushFilters = _filters.filterNot(f => leftPlan.filters.contains(f) || rightPlan.filters.contains(f))

          // 找到连接这两个子图的边
          val candidates = graph.findConnectingEdges(left, right).toSeq.flatMap {
            // 为每个连接边创建连接计划
            edge => createJoinPlan(graph, pushFilters, left, right, edge, leftPlan, rightPlan)
          }

          // 计算连接计划的成本
          if (candidates.nonEmpty) {
            val good = candidates.map(_.withFilters(_filters)).map(estimate).minBy(_.cost)
            // 更新最佳计划
            bestPlan = bestPlan match {
              case None => Some(good)
              case Some(best) if good.cost < best.cost => Some(good)
              case _ => bestPlan
            }
          }
        }
      }
    }

    // 存储子图的最优计划
    if (bestPlan.isDefined) {
      dpTable.put(subset, bestPlan.get)
    }
  }

  // 将子图划分为两个连通子图
  private def partitionSubset(graph: GraphPattern, subset: Set[GraphPatternNode], leftSize: Int): Set[(Set[GraphPatternNode], Set[GraphPatternNode])] = {
    val result = mutable.Set[(Set[GraphPatternNode], Set[GraphPatternNode])]()

    // 生成所有大小为leftSize的子集
    val leftSubsets = subset.subsets(leftSize).filter(isConnected(graph, _))

    for (left <- leftSubsets) {
      val right = subset -- left

      // 确保右侧子集也是连通的
      if (isConnected(graph, right)) {
        result.add((left, right))
      }
    }

    result.toSet
  }

  // 检查子图是否连通
  private def isConnected(graph: GraphPattern, subset: Set[GraphPatternNode]): Boolean = {
    if (subset.isEmpty) return true
    if (subset.size == 1) return true

    val visited = mutable.Set[GraphPatternNode]()
    val queue = mutable.Queue[GraphPatternNode]()

    // 从第一个节点开始BFS
    val start = subset.head
    queue.enqueue(start)
    visited.add(start)

    while (queue.nonEmpty) {
      val current = queue.dequeue()

      // 获取当前节点在子集中的邻居
      val neighbors = graph.neighbors(current).intersect(subset)

      for (neighbor <- neighbors if !visited.contains(neighbor)) {
        visited.add(neighbor)
        queue.enqueue(neighbor)
      }
    }

    // 如果访问到的节点数等于子集大小，则子图是连通的
    visited.size == subset.size
  }

  // 找到连接两个子图的边
//  private def findConnectingEdges(graph: GraphPattern, left: Set[GraphPatternNode], right: Set[GraphPatternNode]): Set[GraphPatternEdge] = {
//    val connectingEdges = mutable.Set[GraphPatternEdge]()

//    for (leftNode <- left) {
//      // 获取左侧节点的所有边
//      val edges = graph.edgesOf(leftNode)
//
//      for (edge <- edges) {
//        // 检查边是否连接到右侧子图的节点
//        val neighbors = graph.neighbors(leftNode)
//
//        for (neighbor <- neighbors if right.contains(neighbor)) {
//          connectingEdges.add(edge)
//        }
//      }
//    }

//    connectingEdges.toSet
//  }

  // 创建连接两个子计划的物理计划
  private def createJoinPlan(graph: GraphPattern,
                             filters: Seq[Expression],
                             leftNodes: Set[GraphPatternNode],
                             rightNodes: Set[GraphPatternNode],
                             edge: GraphPatternEdge, leftPlan: Candidate, rightPlan: Candidate)
                            (implicit ppc: PhysicalPlannerContext ): Seq[Candidate] = {
    // 找到边连接的源节点和目标节点
    val (sourceNode, targetNode) = graph.nodesOf(edge)
    val left2right = (sourceNode, targetNode) match {
      case (left, right) if leftNodes.contains(left) && rightNodes.contains(right) => true
      case (right, left) if leftNodes.contains(left) && rightNodes.contains(right) => false
      case _ => throw LynxException("error ")
    }
    val(_sourceNode, _edge, _targetNode) = if (left2right) {
      (sourceNode, edge, targetNode)
    } else {
      (targetNode, edge.reversed, sourceNode)
    }
    val expandFactory = ExpandFactory(_sourceNode, _edge, _targetNode)
    val defaultTriplePlanner: TriplePlanner = DefaultTriplePlanner(_sourceNode, _edge, _targetNode)
    val inferPlanner: InferPlanner = InferPlanner()

    val plans: Seq[PhysicalPlan] = (sourceNode.virtual, edge.virtual, targetNode.virtual, left2right) match {
      case (_, true, true, true)
        => inferPlanner.filters(_targetNode)(leftPlan.plan ~> InferExpand(_edge, _targetNode)) // todo infer props
      case (true, true, false, true) => Seq() // todo infer link
//      case (false, true, true) => Seq(leftPlan.plan ~> InferExpand(_edge, _targetNode) ) // todo infer props
      case (false, false, false, _) => (leftNodes.size, rightNodes.size) match {
        // 1. (a), (b) => (a) -> (b), a expand b, or relationships(a, b)
        case (1, 1) =>
          // 1.0 relationships
          defaultTriplePlanner.plan ++
            // 1.1 expand
            Seq(leftPlan.plan ~> expandFactory.expand ~> DefaultNodePlanner(rightNodes.head).makeFilter)
        // 2. (n, 1) or (1, n)
        case (_, 1) => Seq(leftPlan.plan ~> expandFactory.expand ~> DefaultNodePlanner(rightNodes.head).makeFilter)
        case (1, _) => Seq(rightPlan.plan ~> expandFactory.reversed.expand ~> DefaultNodePlanner(leftNodes.head).makeFilter)
        // 3. (n, n): a join b
        case (_, _) => Seq.empty // TODO Join
      }
      case _ => Seq.empty
    }
    // TODO edge filter of type and props
    // push last filters
    val f = Filter.multi(filters)
    plans.map(_ ~> f).map(p => Candidate(p).withFilters(filters))
  }

}
