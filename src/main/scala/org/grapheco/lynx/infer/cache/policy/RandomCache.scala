package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy, Entry}

import scala.util.Random
import scala.collection.mutable

class RandomCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "Random"
  private val map = mutable.HashMap[K, Entry[V]]()
  private val rnd = new Random()
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
    map.get(key).map { e =>
      hits += 1L
      hitCost += e.cost
      e.value
    }
  }

  override def onPut(key: K, value: V, cost: Long): Set[(K,V,Int)] = {
    val size = valueSize(value)
    computeCost += cost
    map.get(key) match {
      case Some(e) =>
        e.value = value
        e.cost = cost
        cacheSize += size - e.size
        e.size = size
      case None =>

        map += key -> Entry(value, cost, freq = 1, size = size)
        cacheSize += size
    }
    while (cacheSize >= capacity) {
      val victim = map.keysIterator.drop(rnd.nextInt(map.size)).next()
      map.get(victim).foreach{e => cacheSize -= e.size; evictionsCost += e.cost}
      map -= victim;
      evictions += 1
    }
    Set.empty // TODO
  }

  override def invalidate(keys: Iterable[K]): Unit = {
    keys.foreach{k =>
      map.get(k).foreach(cacheSize -= _.size)
      invalidations += 1
      map.remove(k)
    }
  }

  override def size: Int = cacheSize
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys

  override def metrics: CacheMetrics = CacheMetrics(gets, hits, computeCost, hitCost, invalidations, evictions, evictionsCost)
}

