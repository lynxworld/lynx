package org.grapheco.lynx.infer.cache.core

trait CachePolicy[K, V] {
  def name: String
  def capacity: Int
  def onGet(key: K, tick: Long): Option[V]
  def onPut(key: K, value: V, cost: Long, tick: Long): Option[(K,V)]
  def invalidate(keys: Iterable[K], tick: Long): Unit
  def size: Int
  def contains(key: K): Boolean
  def allKeys: Iterable[K]
  def statsSnapshot: Map[String, Any] = Map("size" -> size)
  def evict(key: K): Option[(K,V)] = None
}

// 通用条目
case class Entry[V](
                     var value: V,
                     var cost: Long,
                     var freq: Int = 0,
                     var lastAccessTick: Long = -1L,
                     var priority: Double = 0.0,        // 各策略可复用
                     var lastPutTick: Long = -1L
                   )
