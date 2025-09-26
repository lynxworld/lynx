package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}

import scala.collection.mutable

// 简单 LFU：同频率用 LRU（双哈希）
class LFUCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "LFU"
  private case class FreqNode(freq: Int, keys: mutable.LinkedHashSet[K])
  private val entries = mutable.HashMap[K, Entry[V]]()
  private val freqMap = mutable.HashMap[Int, FreqNode]()
  private var minFreq = 0
  private var evictions = 0L

  override def onGet(key: K, tick: Long): Option[V] =
    entries.get(key).map { e =>
      bumpFreq(key, e)
      e.lastAccessTick = tick
      e.freq += 1
      e.value
    }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Unit = {
    if (capacity <= 0) return
    entries.get(key) match {
      case Some(e) =>
        e.value = value
        e.cost = cost
        e.lastPutTick = tick
        onGet(key, tick)
      case None =>
        if (entries.size >= capacity) evict()
        val e = Entry(value, cost, freq = 1, lastAccessTick = tick, lastPutTick = tick)
        entries += key -> e
        val node = freqMap.getOrElseUpdate(1, FreqNode(1, mutable.LinkedHashSet.empty[K]))
        node.keys += key
        minFreq = 1
    }
  }

  private def bumpFreq(key: K, e: Entry[V]): Unit = {
    val oldFreq = e.freq.max(1)
    val oldNode = freqMap(oldFreq)
    oldNode.keys -= key
    if (oldFreq == minFreq && oldNode.keys.isEmpty) minFreq += 1
    val newFreq = oldFreq + 1
    val newNode = freqMap.getOrElseUpdate(newFreq, FreqNode(newFreq, mutable.LinkedHashSet.empty[K]))
    newNode.keys += key
  }

  private def evict(): Unit = {
    freqMap.get(minFreq).foreach { node =>
      val victim = node.keys.head
      node.keys -= victim
      entries -= victim
      evictions += 1
      if (node.keys.isEmpty) freqMap -= node.freq
    }
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach { k =>
      entries.get(k).foreach { e =>
        val f = e.freq.max(1)
        freqMap.get(f).foreach(_.keys -= k)
        entries -= k
      }
    }

  override def size: Int = entries.size
  override def contains(key: K): Boolean = entries.contains(key)
  override def allKeys: Iterable[K] = entries.keys
  override def statsSnapshot: Map[String, Any] = Map("size" -> size, "evictions" -> evictions)
}
