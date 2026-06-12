package org.grapheco.lynx.infer.cache.core

import org.grapheco.lynx.infer.cache.core.NoneInferCache.{CacheKey, CacheValue}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship}

trait InferCache {
  type CacheKey = (Int, Long)
  type CacheValue = List[(LynxValue, LynxValue)]
  type CacheValueProp = List[(LynxString, LynxValue)]
  type CacheValueExpand = List[(LynxRelationship, LynxNode)]
  type CacheValueLink = List[(LynxRelationship, LynxInteger)]

  def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit
  def get(cacheKey: CacheKey): Option[CacheValue]
  def metrics: CacheMetrics
  def currentSize: Long

  def putProp(nodeId: Long, inferCode: Int, propKeyValue: List[(LynxPropertyKey,LynxValue)], cost: Long): Unit = {
    val key = (inferCode, nodeId)
    put(key, propKeyValue.map(kv => (LynxString(kv._1.value), kv._2)), cost)
  }

  def putExpand(nodeId: Long, inferCode: Int, outEdges: List[(LynxRelationship, LynxNode)], cost: Long): Unit = {
    val key = (inferCode, nodeId)
    put(key, outEdges, cost)
  }

  def putLink(nodeId: Long, inferCode: Int, linkValues: List[(LynxRelationship, LynxInteger)], cost: Long): Unit = {
    val key = (inferCode, nodeId)
    put(key, linkValues, cost)
  }

  def getProp(nodeId: Long, inferCode: Int): List[(LynxPropertyKey, LynxValue)] = {
    val key = (inferCode, nodeId)
    get(key) match {
      case Some(values) =>
        values.collect {
          case (LynxString(propKey), propValue) => (LynxPropertyKey(propKey), propValue)
        }
      case None => List.empty
    }
  }

  def getExpand(nodeId: Long, inferCode: Int): List[(LynxRelationship, LynxNode)] = {
    val key = (inferCode, nodeId)
    get(key) match {
      case Some(values) =>
        values.collect {
          case (rel: LynxRelationship, node: LynxNode) => (rel, node)
        }
      case None => List.empty
    }
  }

  def getLink(nodeId: Long, inferCode: Int): List[(LynxRelationship, LynxInteger)] = {
    val key = (inferCode, nodeId)
    get(key) match {
      case Some(values) =>
        values.collect {
          case (rel: LynxRelationship, intValue: LynxInteger) => (rel, intValue)
        }
      case None => List.empty
    }
  }
}

object NoneInferCache extends InferCache {
  override def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit = {}

  override def get(cacheKey: CacheKey): Option[CacheValue] = None

  override def metrics: CacheMetrics = CacheMetrics()

  override def currentSize: Long = 0L
}

class DefaultInferCache(val _cache: CachePolicy[CacheKey, CacheValue],
                        val Cascading: Boolean = false) extends InferCache {

  private var evictedCosts = List[Int]()

  val depGraph = Dep._depGraph

  def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit = {
    val evicted = _cache.onPut(cacheKey, cacheValue, cost)
    if (Cascading && evicted.nonEmpty) {
      val (exp, deps) = depGraph.head
      evicted.filter(_._1._1 == exp).foreach { case (key, value: CacheValueExpand, _) =>
        value.foreach{ case(_, node) =>
          val nodeId = node.id.toLynxInteger.v
          val childKeys = deps.map(dep => (dep, nodeId))
          _cache.invalidate(childKeys)
        }
      }
    }
    evictedCosts = evictedCosts ++ evicted.map(_._3)
  }

  def get(cacheKey: CacheKey): Option[CacheValue] = {
    val maybe = _cache.onGet(cacheKey)
    maybe
  }

  override def metrics: CacheMetrics = _cache.metrics

  override def currentSize: Long = _cache.size
}

object InferCache {
  def apply(cache: Option[CachePolicy[CacheKey, CacheValue]], useDepGraph: Boolean): InferCache = {
    cache match {
      case Some(c) => new DefaultInferCache(c, Cascading = useDepGraph)
      case None => NoneInferCache
    }
  }
}
