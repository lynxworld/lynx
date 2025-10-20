package org.grapheco.lynx.infer.cache

import org.grapheco.lynx.infer.cache.CacheKey.Prop
import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy}
import org.grapheco.lynx.infer.cache.depgraph.DepGraph
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}

import scala.collection.mutable

object Meta {
  val LABEL: Int = "$LABEL".hashCode

  val ALIVE: Int = "$ALIVE".hashCode

  val PROPS: Int = "$PROPS".hashCode

  def getCode(pk: LynxPropertyKey): Int = pk.value.hashCode

  def getCode(label: LynxNodeLabel): Int = label.value.hashCode

  def getCode(relType: LynxRelationshipType): Int = relType.value.hashCode
}

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

class DefaultInferCache(val _cache: CachePolicy[CacheKey, CacheValue],
                        val dependencyGraph: DepGraph = DepGraph.empty) extends InferCache {

  private var request: Int = 0

  private var gets: Int = 0

  private var hits: Int = 0

  private final val TIME0 = System.currentTimeMillis()

  private def tick: Long = System.currentTimeMillis() - TIME0

  private var cost: Double = 0.0

  private var hitCost: Double = 0.0

  def put(cacheKey: CacheKey, cacheValue: CacheValue, cost: Long): Unit = {
    request+=1
    val evicted = _cache.onPut(cacheKey, cacheValue, cost, tick)
    evicted.foreach{case (k,v) => evictDep(k,v)}
    this.cost += cost
  }

  private def evictDep(key: CacheKey, value: CacheValue): Unit = (key, value) match {
      case (k:CacheKey.Expand, v:CacheValue.ExpandValue) => v.values.foreach{
        case (relationship, node) => // remove node
          val id = node.id
          if (_cache.contains(aliveKey(id.toLynxInteger.v))) {
            val ps = props(id.toLynxInteger.v)
            _cache.evict(propsKey(id.toLynxInteger.v))
            ps.value.collect{
              case LynxInteger(i) => i
            }.foreach{ propCode =>
              _cache.evict(Prop(id.toLynxInteger.v, propCode)).foreach( kv => evictDep(kv._1, kv._2) )
            }
          }
      }
      case (k: CacheKey.Prop, v: CacheValue.PropValue) => {
        // remove from list
        val ps = props(k.id)
        val nps = ps.value.filterNot(_ == LynxInteger(k.prop))
        _cache.onPut(propsKey(k.id), CacheValue.PropValue(LynxList(nps)), 0, tick)
        dependencyGraph.getOutNeighbors(k.prop.toInt).foreach{ dep =>
//          dep.typo match {
//            case 0 => // node
//              _cache.evict(aliveKey(k.id)).foreach( kv => evictDep(kv._1, kv._2) )
//            case 2 => // prop
//              _cache.evict(Prop(k.id, dep.code.toLong)).foreach( kv => evictDep(kv._1, kv._2) )
//            case _ => // do nothing
//          }
        }
      }
  }

  def aliveKey(id: Long) = CacheKey.Prop(id, Meta.ALIVE.toLong)

  def propsKey(id: Long) = CacheKey.Prop(id, Meta.PROPS.toLong)

  def props(id: Long): LynxList =
    _cache.onGet(propsKey(id), tick).collect{
      case CacheValue.PropValue(value: LynxList) => value
    }.getOrElse(LynxList.apply(List.empty))

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
