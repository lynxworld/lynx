package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.logical.plans.GraphPatternNode

import scala.collection.mutable

/**
 * 动态规划表，用于存储子问题的最优解
 * 键为节点集合，值为最优的物理计划及其成本
 */
class DPTable {
  private val table = mutable.Map[Set[GraphPatternNode], Candidate]()

  /**
   * 添加或更新子图的最优计划
   *
   * @param subset    子图节点集合
   * @param candidate 计划候选项
   */
  def put(subset: Set[GraphPatternNode], candidate: Candidate): Unit = {
    println(s"put ${subset.map(_.variableName)} -> $candidate")
    table(subset) = candidate
  }

  /**
   * 获取子图的最优计划
   *
   * @param subset 子图节点集合
   * @return 计划候选项
   */
  def get(subset: Set[GraphPatternNode]): Option[Candidate] = {
    table.get(subset)
  }

  /**
   * 检查子图是否已有最优计划
   *
   * @param subset 子图节点集合
   * @return 是否存在
   */
  def contains(subset: Set[GraphPatternNode]): Boolean = {
    table.contains(subset)
  }

  /**
   * 获取所有已计算的子图
   *
   * @return 子图集合
   */
  def subsets: Set[Set[GraphPatternNode]] = {
    table.keySet.toSet
  }

  /**
   * 获取表中条目数量
   *
   * @return 条目数量
   */
  def size: Int = {
    table.size
  }

  /**
   * 清空表
   */
  def clear(): Unit = {
    table.clear()
  }

  /**
   * 获取表中所有条目
   *
   * @return 所有条目
   */
  def entries: Iterable[(Set[GraphPatternNode], Candidate)] = {
    table.toIterable
  }

  /**
   * 获取指定子图的最优计划，如果不存在则抛出异常
   *
   * @param subset 子图节点集合
   * @return 计划候选项
   */
  def apply(subset: Set[GraphPatternNode]): Candidate = {
    table(subset)
  }

  /**
   * 获取表的字符串表示
   *
   * @return 表的字符串表示
   */
  override def toString: String = {
    val sb = new StringBuilder("DPTable {\n")
    for ((subset, candidate) <- table) {
      sb.append(s"  ${subset.map(_.variableName).mkString(", ")} -> Cost: ${candidate.cost}, Cardinal: ${candidate.cardinal}\n")
    }
    sb.append("}")
    sb.toString
  }
}
