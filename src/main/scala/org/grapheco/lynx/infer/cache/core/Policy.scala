package org.grapheco.lynx.infer.cache.core

import org.grapheco.lynx.infer.cache.core.NoneInferCache.CacheValue

trait CachePolicy[K, V] {
  def name: String
  def capacity: Int
  def onGet(key: K): Option[V]
  def onPut(key: K, value: V, cost: Long): Set[(K,V, Int)]
  def invalidate(keys: Iterable[K]): Unit
  def size: Int
  def valueSize(value: V): Int = value match {
    case cv: CacheValue => cv.size
    case _ => 1 // 默认大小为1
  }
  def contains(key: K): Boolean
  def allKeys: Iterable[K]
  def evict(key: K): Option[(K,V)] = None
  def metrics: CacheMetrics
}

// 通用条目
case class Entry[V](var value: V, var cost: Long, var size: Int, var freq: Int = 0)
