package org.grapheco.lynx.infer.cache

import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

import scala.collection.mutable

sealed trait CacheKey {
  def a: Long
  def b: Long
}
object CacheKey {
  final case class Expand(rel: Long, id: Long) extends CacheKey {
    val a: Long = rel; val b: Long = id
  }
  final case class Prop(id: Long, prop: Long) extends CacheKey {
    val a: Long = id; val b: Long = prop
  }
  final case class Link(rel: Long, id: Long) extends CacheKey {
    val a: Long = rel; val b: Long = id
  }
}

sealed trait CacheValue
object CacheValue {
  final case class ExpandValue(values: List[(LynxRelationship, LynxNode)]) extends CacheValue
  final case class PropValue(value: LynxValue) extends CacheValue
  final case class LinkValue(rels: List[(LynxRelationship, LynxNode)]) extends CacheValue
}
trait InferCache {
  final val LABEL = "$LABEL".hashCode.toLong
  def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit
  def get(cacheKey: CacheKey): Option[CacheValue]
  def metrics: CacheMetrics
}

object NoneInferCache extends InferCache {
  override def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit = {}

  override def get(cacheKey: CacheKey): Option[CacheValue] = None

  override def metrics: CacheMetrics = CacheMetrics()
}

class DefaultInferCache(val _cache: CachePolicy[CacheKey, CacheValue]) extends InferCache {

  private var request: Int = 0

  private var gets: Int = 0

  private var hits: Int = 0

  private final val TIME0 = System.currentTimeMillis()

  private def tick: Long = System.currentTimeMillis() - TIME0

  private var cost: Double = 0.0

  private var hitCost: Double = 0.0

//  private val negativeCache = mutable.HashSet[CacheKey]

  def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit = {
    request+=1
    _cache.onPut(cacheKey, cacheValue, cost, tick)
    this.cost += cost
  }

  def get(cacheKey: CacheKey): Option[CacheValue] = {
    request+=1
    gets+=1
    val maybe = _cache.onGet(cacheKey, tick)
    if (maybe.isDefined){hits+=1}
    maybe
  }

  override def metrics: CacheMetrics = CacheMetrics(
    totalReq = request,
    gets = gets,
    hits = hits,
    recomputeCost = cost,
    evictions = _cache.statsSnapshot.get("evictions").map(_.asInstanceOf[Long]).getOrElse(0L)
  )
}
