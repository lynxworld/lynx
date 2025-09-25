package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}
import org.grapheco.lynx.runner.infercache.core._

import scala.collection.mutable

/**
 * LDC-E v2.0:
 *  - L: LRU 链表提供“最冷优先”候选集
 *  - D: Dependency 出度优先保留
 *  - C-E: Cost + Frequency + (Time/Recency) 价值函数评分驱逐
 *
 * @param capacity   最大条目数
 * @param candidateK 每次驱逐评分的候选集大小K（从尾部开始取）
 * @param Wc         成本权重乘子
 * @param Wf         频率权重
 * @param Wt         时间（Recency 反函数）权重
 * @param dependencyOutDegreeProvider 若 put 未提供出度，则调用该函数
 */
class LDCECache[K, V](
                         val capacity: Int,
                         val candidateK: Int = 16,
                         val Wc: Double = 1.0,
                         val Wf: Double = 1.0,
                         val Wt: Double = 1.0,
                         dependencyOutDegreeProvider: Option[K => Int] = None
                       ) extends CachePolicy[K, V] {

  override val name: String = s"LDC-E(k=$candidateK,Wc=$Wc,Wf=$Wf,Wt=$Wt)"

  // ------------------------
  // 内部节点定义
  // ------------------------
  private case class Node(
                           key: K,
                           var entry: Entry[V],
                           var prev: Node = null,
                           var next: Node = null
                         )

  private val map = mutable.HashMap[K, Node]()
  private var head: Node = null
  private var tail: Node = null

  // 统计
  private var _evictions: Long = 0L
  private var _candidateEvaluations: Long = 0L
  private var _evictCalls: Long = 0L

  // ---------------------------------
  // 对外基础接口实现
  // ---------------------------------
  override def onGet(key: K, tick: Long): Option[V] = {
    map.get(key) match {
      case Some(node) =>
        val e = node.entry
        // 增量更新
        e.freq += 1
        e.lastAccessTick = tick
        // 访问后移到头部
        moveToHead(node)
        Some(e.value)
      case None => None
    }
  }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Unit = {
    map.get(key) match {
      case Some(node) =>
        // 更新已有条目
        val e = node.entry
        e.value = value
        e.cost = cost
        // 新写入也相当于一次访问
        e.freq += 1
        e.priority = 0
        moveToHead(node)
      case None =>
        val e = Entry(value, cost, freq = 1, lastAccessTick = tick)
        val n = Node(key, e)
        addToHead(n)
        map += key -> n
        // 可能触发驱逐
        if (map.size > capacity) {
          evictOne(tick)
        }
    }
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach { k =>
      map.get(k).foreach { n =>
        removeNode(n); map -= k
      }
    }

  override def size: Int = map.size
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys

  override def statsSnapshot: Map[String, Any] = Map(
    "size" -> size,
    "evictions" -> _evictions,
    "evictCalls" -> _evictCalls,
    "candidateEvaluations" -> _candidateEvaluations
  )

  // ---------------------------------
  // 驱逐核心逻辑
  // ---------------------------------
  private def evictOne(nowTick: Long): Unit = {
    _evictCalls += 1
    if (tail == null) return

    val candidates = collectTailCandidates(candidateK)
    if (candidates.isEmpty) return

    // 依赖出度过滤
//    val zeroDep = candidates.filter(_.entry.dependencyOutDegree == 0)
    val zeroDep = List.empty
    val evictable = if (zeroDep.nonEmpty) zeroDep else candidates

    var minNode: Node = null
    var minScore = Double.MaxValue

    evictable.foreach { node =>
      val sc = valueScore(node.entry, nowTick)
//      node.entry.lastScore = sc
      _candidateEvaluations += 1
      if (sc < minScore) { minScore = sc; minNode = node }
    }

    if (minNode != null) {
      removeNode(minNode); map -= minNode.key
      _evictions += 1
    }
  }

  /**
   * 从尾部开始收集最多 K 个节点（不移除）
   */
  private def collectTailCandidates(k: Int): List[Node] = {
    val res = mutable.ListBuffer.empty[Node]
    var cur = tail
    var i = 0
    while (cur != null && i < k) {
      res.+=(cur)
      cur = cur.prev
      i += 1
    }
    res.toList
  }

  /**
   * 价值评分函数（越低越容易被驱逐）
   */
  private def valueScore(e: Entry[V], nowTick: Long): Double = {
    val age = (nowTick - e.lastAccessTick).max(0L)
    val costFactor = math.log1p(Wc * e.cost.toDouble)               // log(1 + Wc*C)
    val freqTerm   = Wf * math.log1p(e.freq.toDouble)               // Wf * log(1 + F)
    val recencyTerm= Wt / (age + 1.0)                               // Wt / (age+1)
    costFactor * (freqTerm + recencyTerm)
  }

  // ---------------------------------
  // 链表操作
  // ---------------------------------
  private def addToHead(n: Node): Unit = {
    n.prev = null
    n.next = head
    if (head != null) head.prev = n
    head = n
    if (tail == null) tail = n
  }

  private def removeNode(n: Node): Unit = {
    if (n.prev != null) n.prev.next = n.next else head = n.next
    if (n.next != null) n.next.prev = n.prev else tail = n.prev
    n.prev = null; n.next = null
  }

  private def moveToHead(n: Node): Unit = {
    if (n eq head) return
    removeNode(n)
    addToHead(n)
  }
}
