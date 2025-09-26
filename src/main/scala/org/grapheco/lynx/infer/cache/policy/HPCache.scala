package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}

import scala.collection.mutable

// 优先级公式：P = α * Recency + β * FreqScore - γ * log(cost)
class HPCache[K, V](
                     val capacity: Int,
                     alpha: Double, beta: Double, gamma: Double,
                     recencyHalfLife: Double = 100 // 控制 recency 衰减尺度
                   ) extends CachePolicy[K, V] {

  override val name: String = s"HP-C(a=$alpha,b=$beta,g=$gamma)"
  private val map = mutable.HashMap[K, Entry[V]]()
  private implicit val ord: Ordering[(K, Double)] = Ordering.by(_._2) // 小顶
  private val heap = mutable.PriorityQueue.empty[(K, Double)](ord.reverse)
  private var evictions = 0L

  override def onGet(key: K, tick: Long): Option[V] =
    map.get(key).map { e =>
      e.freq += 1
      e.lastAccessTick = tick
      e.priority = score(e, tick)
      heap.enqueue(key -> e.priority)
      e.value
    }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Unit = {
    val e = map.getOrElseUpdate(key, Entry(value, cost))
    e.value = value
    e.cost = cost
    e.lastPutTick = tick
    e.lastAccessTick = tick
    e.freq += 1
    e.priority = score(e, tick)
    heap.enqueue(key -> e.priority)
    if (map.size > capacity) evict()
  }

  private def evict(): Unit = {
    var removed = false
    while (!removed && heap.nonEmpty) {
      val (k, p) = heap.dequeue()
      map.get(k) match {
        case Some(e) if e.priority == p =>
          map -= k; evictions += 1; removed = true
        case _ => // stale heap entry, skip
      }
    }
  }

  private def score(e: Entry[V], nowTick: Long): Double = {
    val age = (nowTick - e.lastAccessTick).max(1)
    val recency = 1.0 / (1.0 + age / recencyHalfLife)
    val f = e.freq.toDouble
    val freqScore = f / (f + 10.0)
    val costScore = math.log(e.cost.toDouble.max(1))
    alpha * recency + beta * freqScore - gamma * costScore
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach(map.remove)

  override def size: Int = map.size
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys
  override def statsSnapshot: Map[String, Any] = Map("size" -> size, "evictions" -> evictions)
}
