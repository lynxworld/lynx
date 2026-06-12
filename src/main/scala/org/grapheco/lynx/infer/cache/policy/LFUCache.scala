package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy, Entry}

import scala.collection.mutable

// 简单 LFU：同频率用 LRU（双哈希）
class LFUCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "LFU"
  private case class FreqNode(freq: Int, keys: mutable.LinkedHashSet[K])
  private val entries = mutable.HashMap[K, Entry[V]]()
  private val freqMap = mutable.HashMap[Int, FreqNode]()
  private var minFreq = 0
  private var cacheSize:Int = 0
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
    entries.get(key).map { e =>
      bumpFreq(key, e)
      e.freq += 1
      hits += 1L
      hitCost += e.cost
      e.value
    }
  }

  override def onPut(key: K, value: V, cost: Long): Set[(K,V, Int)] = {
    val size = valueSize(value)
    computeCost += cost
    entries.get(key) match {
      case Some(e) =>
        e.value = value
        e.cost = cost
        cacheSize += size - e.size
        e.size = size
        onGet(key)
      case None =>
        val e = Entry(value, cost, freq = 1, size = size)
        entries += key -> e
        val node = freqMap.getOrElseUpdate(1, FreqNode(1, mutable.LinkedHashSet.empty[K]))
        node.keys += key
        minFreq = 1
        cacheSize += size
    }
    while (cacheSize >= capacity) evict()
    Set.empty // TODO
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

  private def evict(): Option[(K, V, Int)] = {
    freqMap.get(minFreq).foreach { node =>
      val victim = node.keys.head
      node.keys -= victim
      entries.get(victim).foreach{ e =>
        cacheSize -= e.size;
        evictionsCost += e.cost
      }
      entries -= victim
      evictions += 1
      if (node.keys.isEmpty) freqMap -= node.freq
      None
    }
    None
  }

  override def invalidate(keys: Iterable[K]): Unit = {
    keys.foreach { k =>
      entries.get(k).foreach { e =>
        invalidations += 1
        val f = e.freq.max(1)
        freqMap.get(f).foreach(_.keys -= k)
        cacheSize -= e.size
        entries -= k
      }
    }
  }

  override def size: Int = cacheSize
  override def contains(key: K): Boolean = entries.contains(key)
  override def allKeys: Iterable[K] = entries.keys
  override def metrics: CacheMetrics = CacheMetrics(gets, hits, computeCost, hitCost, invalidations, evictions, evictionsCost)
}
