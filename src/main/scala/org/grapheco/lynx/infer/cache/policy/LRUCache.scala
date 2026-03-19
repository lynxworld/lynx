package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy, Entry}

import scala.collection.mutable

class LRUCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "LRU"

  private case class Node(k: K, var prev: Node = null, var next: Node = null)

  private val map = mutable.HashMap[K, (Entry[V], Node)]()
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

  override def onGet(key: K): Option[V] = {
    gets += 1L
    map.get(key).map { case (e, n) =>
      moveToHead(n)
      hits += 1L
      hitCost += e.cost
      e.value
    }
}

  override def onPut(key: K, value: V, cost: Long): Set[(K,V, Int)] = {
    val size = valueSize(value)
    computeCost += cost
    map.get(key) match {
      case Some((e, n)) =>
        e.value = value
        e.cost = cost
        cacheSize += size - e.size
        e.size = size
        moveToHead(n)
      case None =>
        val e = Entry(value, cost, freq = 1, size = size)
        val node = Node(key)
        map += key -> (e, node)
        addToHead(node)
        cacheSize += size
    }
    val evicted: Set[(K,V, Int)] = Set.empty
    while (cacheSize > capacity) {
      evictTail()
    }
    evicted //TODO
  }

  override def invalidate(keys: Iterable[K]): Unit = {
    keys.foreach { k =>
      map.get(k).foreach { case (e, n) => removeNode(n); map -= k; cacheSize -= e.size;invalidations += 1 }
    }
  }

  private def evictTail(): Unit = if (tail != null) {
    val k = tail.k
    removeNode(tail)
    map.get(k).foreach{ case (e, _) => cacheSize -= e.size; evictionsCost += e.cost }
    map -= k
    evictions += 1
  }

  private def addToHead(n: Node): Unit = {
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
    removeNode(n); addToHead(n)
  }

  override def size: Int = cacheSize
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys
  def all: Iterable[(K, V)] = map.map { case (k, (e, _)) => (k, e.value)}

  override def metrics: CacheMetrics = CacheMetrics(gets, hits, computeCost, hitCost, invalidations, evictions, evictionsCost)
}
