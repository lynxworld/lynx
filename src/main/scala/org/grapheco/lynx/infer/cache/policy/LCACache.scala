package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.NoneInferCache.{CacheKey, CacheValue}
import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy, Dep, Entry}

import scala.collection.mutable

/**
 * LCA (LRU-Cost-Aware) 策略:
 * 仿照论文中的LCA (原LDC) 设计，并适配CachePolicy接口。
 *
 * - L: LRU 链表提供“最冷优先”候选集
 * - C: Cost-Aware 静态优先级分数
 *
 * 核心逻辑：
 * 1. 维护一个LRU双向链表 (HashMap + DoublyLinkedList) 来跟踪访问顺序。
 * 2. 在 `onPut` (插入或更新) 时，根据条目的 `cost` 计算一个 *静态优先级分数* (staticScore)，并将其存储在Node中。
 * 3. 当需要驱逐时 (`evictOne`)，从LRU链表的尾部（最冷）收集 `candidateK` 个候选条目。
 * 4. 驱逐这 K 个条目中 *staticScore* 最低的条目。
 *
 * @param capacity   最大条目数
 * @param candidateK 每次驱逐评分的候选集大小K（从尾部开始取）
 * @param alpha      LCA分数公式中 'Cost' 项的权重 (a)
 * @param beta       LCA分数公式中 'Size' 项的权重 (b) - [注意：因接口限制，此项未使用]
 * @param gamma      LCA分数公式中 'Dependency' 项的权重 (g) - [注意：因接口限制，此项未使用]
 */
class LCACache[K, V](
                      val capacity: Int,
                      val candidateK: Double = 0.1,
                      val alpha: Double = 0.5,
                      val beta: Double = 0.3,
                    ) extends CachePolicy[K, V] {

  override val name: String = s"LCA(k=$candidateK-a=$alpha-b=$beta)"

  // ------------------------
  // 内部节点定义
  // ------------------------
  private case class Node(
                           key: K,
                           var entry: Entry[V],
                           // LCA核心：存储在插入/更新时计算的静态分数
                           var staticScore: Double,
                           var prev: Node = null,
                           var next: Node = null
                         )

  private val map = mutable.HashMap[K, Node]()
  private var head: Node = null
  private var tail: Node = null

  private var cacheSize: Int = 0
  // state metrics
  private var gets: Long = 0L
  private var hits: Long = 0L
  private var computeCost: Double = 0.0
  private var hitCost: Double = 0.0
  private var invalidations: Long = 0L
  private var evictions: Long = 0L
  private var evictionsCost: Long = 0L

  /**
   * LCA 静态优先级分数计算函数
   */
  private def lcaScore(cost: Long, value: V, key: K): Double = {
    val esize = valueSize(value)
    val dep = key match {
      case ck: CacheKey => Dep.getDep(ck._1)
      case _ => 0
    }
    val costFactor = alpha * math.log1p(cost.toDouble + dep * esize)
    val sizeFactor = beta * math.log1p(esize.toDouble)
    costFactor - sizeFactor
  }

  // ---------------------------------
  // 对外基础接口实现
  // ---------------------------------
  override def onGet(key: K): Option[V] = {
    gets += 1L
    map.get(key) map { node =>
        val e = node.entry
        // 访问后移到头部
        moveToHead(node)
        hits += 1L
        hitCost += e.cost
        e.value
    }
  }

  override def onPut(key: K, value: V, cost: Long): Set[(K,V,Int)] = {
    val size = valueSize(value)
    computeCost += cost

    map.get(key) match {
      case Some(node) =>
        val e = node.entry
        e.value = value
        e.cost = cost
        cacheSize += size - e.size
        e.size = size
        node.staticScore = lcaScore(cost, value, key)
        moveToHead(node)

      case None =>
        val e = Entry(value, cost, freq = 1, size = size)
        val staticScore = lcaScore(cost, value, key)
        val n = Node(key, e, staticScore)

        addToHead(n)
        map += key -> n
        cacheSize += size
    }
    val ev = mutable.Set.empty[(K, V, Int)]
    while (cacheSize > capacity) {
      val evicted = evictOne()
      evicted.foreach(e => ev.add(e.key, e.entry.value, e.entry.cost.toInt))
    }
    ev.toSet
  }

  override def invalidate(keys: Iterable[K]): Unit = {
    keys.foreach { k =>
      map.get(k).foreach { n =>
        removeNode(n)
        invalidations += 1
        cacheSize -= n.entry.size
        map -= k
      }
    }
  }

  override def size: Int = cacheSize
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys


  // ---------------------------------
  // 驱逐核心逻辑
  // ---------------------------------
  private def evictOne(): Option[Node] = {
    if (tail == null) return None

    // 1. 从LRU尾部收集K个候选
    val candidates = collectTailCandidates((candidateK*map.size).toInt)
    if (candidates.isEmpty) return None

    var minNode: Node = null
    var minScore = Double.MaxValue

    // 2. 遍历K个候选，找到静态分数最低的
    candidates.foreach { node =>
      // LCA核心：直接读取存储的静态分数
      val sc = node.staticScore
      if (sc < minScore) {
        minScore = sc; minNode = node
      }
    }

    // 3. 驱逐分数最低的节点
    if (minNode != null) {
      removeNode(minNode)
      cacheSize -= minNode.entry.size
      map -= minNode.key
      evictions += 1
      evictionsCost += minNode.entry.cost
    }
    Option(minNode)
  }

  override def evict(key: K): Option[(K, V)] = ???

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

  // ---------------------------------
  // 链表操作 (与LDCE相同)
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

  override def metrics: CacheMetrics = CacheMetrics(gets, hits, computeCost, hitCost, invalidations, evictions, evictionsCost)
}

