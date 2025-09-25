package org.grapheco.lynx.infer.cache.policy

import org.grapheco.lynx.infer.cache.core.{CachePolicy, Entry}
import org.grapheco.lynx.runner.infercache.core._

import scala.util.Random
import scala.collection.mutable

class RandomCache[K, V](val capacity: Int) extends CachePolicy[K, V] {
  override val name: String = "Random"
  private val map = mutable.HashMap[K, Entry[V]]()
  private val rnd = new Random()
  private var evictions = 0L

  override def onGet(key: K, tick: Long): Option[V] =
    map.get(key).map { e =>
      e.lastAccessTick = tick
      e.freq += 1
      e.value
    }

  override def onPut(key: K, value: V, cost: Long, tick: Long): Unit = {
    map.get(key) match {
      case Some(e) =>
        e.value = value
        e.cost = cost
        e.lastAccessTick = tick
        e.lastPutTick = tick
      case None =>
        if (map.size >= capacity) {
          val victim = map.keysIterator.drop(rnd.nextInt(map.size)).next()
          map -= victim; evictions += 1
        }
        map += key -> Entry(value, cost, freq = 1, lastAccessTick = tick, lastPutTick = tick)
    }
  }

  override def invalidate(keys: Iterable[K], tick: Long): Unit =
    keys.foreach(map.remove)

  override def size: Int = map.size
  override def contains(key: K): Boolean = map.contains(key)
  override def allKeys: Iterable[K] = map.keys
  override def statsSnapshot: Map[String, Any] = Map("size" -> size, "evictions" -> evictions)
}

