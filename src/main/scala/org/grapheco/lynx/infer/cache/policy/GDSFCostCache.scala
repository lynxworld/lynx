package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}
import org.grapheco.lynx.runner.infercache.core._

import scala.collection.mutable

// 简化 GDSF：优先级 H = (cost * freqFactor) / sizeLike + L；这里 sizeLike=1，可替换为真实大小
// L = 上次驱逐的 H 值（老化因子）
class GDSFCostCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "GDSF-Cost"
  private val map = mutable.HashMap[K, Entry[V]]()
  private implicit val ord: Ordering[(K, Double)] = Ordering.by(_._2) // 小顶
  private val heap = mutable.PriorityQueue.empty[(K, Double)](ord.reverse)
  private var evictions = 0L
  private var L: Double = 0.0 // aging base

  override def onGet(key: K, tick: Long): Option[V] =
    map.get(key).map { e =>
      e.freq += 1
      e.lastAccessTick = tick
      e.priority = computePriority(e)
      heap.enqueue(key -> e.priority)
      e.value
    }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Unit = {
    val e = map.getOrElseUpdate(key, Entry(value, cost))
    e.value = value
    e.cost = cost
    e.freq += 1
    e.lastPutTick = tick
    e.lastAccessTick = tick
    e.priority = computePriority(e)
    heap.enqueue(key -> e.priority)
    if (map.size > capacity) evict()
  }

  private def computePriority(e: Entry[V]): Double = {
    val sizeLike = 1.0
    val freqFactor = 1.0 + math.log(e.freq.toDouble.max(1))
    L + (e.cost * freqFactor) / sizeLike
  }

  private def evict(): Unit = {
    var removed = false
    while (!removed && heap.nonEmpty) {
      val (k, p) = heap.dequeue()
      map.get(k) match {
        case Some(e) if e.priority == p =>
          L = p // aging
          map -= k
          evictions += 1
          removed = true
        case _ =>
      }
    }
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach(map.remove)

  override def size: Int = map.size
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys
  override def statsSnapshot: Map[String, Any] =
    Map("size" -> size, "evictions" -> evictions, "L" -> L)
}

