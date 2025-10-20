package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}

import scala.collection.mutable

class LRUCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "LRU"

  private case class Node(k: K, var prev: Node = null, var next: Node = null)
  private val map = mutable.HashMap[K, (Entry[V], Node)]()
  private var head: Node = null
  private var tail: Node = null
  private var evictions: Long = 0L

  override def onGet(key: K, tick: Long): Option[V] =
    map.get(key).map { case (e, n) =>
      e.freq += 1
      e.lastAccessTick = tick
      moveToHead(n)
      e.value
    }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Option[(K,V)] = {
    map.get(key) match {
      case Some((e, n)) =>
        e.value = value
        e.cost = cost
        e.lastPutTick = tick
        e.lastAccessTick = tick
        moveToHead(n)
      case None =>
        val e = Entry(value, cost, freq = 1, lastAccessTick = tick, lastPutTick = tick)
        val node = Node(key)
        map += key -> (e, node)
        addToHead(node)
        if (map.size > capacity) evictTail()
    }
    None
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach { k =>
      map.get(k).foreach { case (_, n) => removeNode(n); map -= k }
    }

  private def evictTail(): Unit = if (tail != null) {
    val k = tail.k
    removeNode(tail)
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

  override def size: Int = map.size
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys
  override def statsSnapshot: Map[String, Any] = Map("size" -> size, "evictions" -> evictions)
}
