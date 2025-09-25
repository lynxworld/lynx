package org.grapheco.lynx.infer.cache.sim

import scala.collection.mutable

// 仅用于生成“失效事件” -> 缓存策略会调用 invalidate
class DependencyIndex[K] {
  private val depToKeys = mutable.HashMap[String, mutable.Set[K]]()

  def add(dep: String, key: K): Unit = {
    val set = depToKeys.getOrElseUpdate(dep, mutable.Set.empty[K])
    set += key
  }

  def removeKey(key: K): Unit =
    depToKeys.values.foreach(_ -= key)

  def affected(dep: String): Set[K] =
    depToKeys.get(dep).map(_.toSet).getOrElse(Set.empty)

  def affected(deps: Iterable[String]): Set[K] =
    deps.flatMap(affected).toSet
}

