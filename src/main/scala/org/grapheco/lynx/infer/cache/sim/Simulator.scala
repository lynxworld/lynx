package org.grapheco.lynx.infer.cache.sim

import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy, OpType, Request}
import org.grapheco.lynx.runner.infercache.core._

import scala.collection.mutable

case class SimulationResult(
                             policyName: String,
                             metrics: CacheMetrics,
                             extra: Map[String, Any]
                           )

class Simulator[K, V](
                       policy: CachePolicy[K, V],
                       valueProvider: K => V,
                       dependencyIndex: Option[DependencyIndex[K]] = None
                     ) {

  private val lastEvictedTime = mutable.HashMap[K, Long]()
  private val lastPutTime = mutable.HashMap[K, Long]()
  private var metrics = CacheMetrics()

  def run(trace: Seq[Either[Request[K], (Long, String)]], dependencyMap: Map[K, List[String]] = Map.empty): SimulationResult = {
    trace.foreach {
      case Left(req) => handleRequest(req, dependencyMap)
      case Right((tick, dep)) => handleInvalidation(tick, dep)
    }
    SimulationResult(policy.name, metrics, policy.statsSnapshot)
  }

  private def handleRequest(req: Request[K], dependencyMap: Map[K, List[String]]): Unit = {
    metrics = metrics.copy(totalReq = metrics.totalReq + 1)
    req.op match {
      case OpType.Get =>
        metrics = metrics.copy(gets = metrics.gets + 1)
        policy.onGet(req.key, req.tick) match {
          case Some(_) =>
            metrics = metrics.copy(
              hits = metrics.hits + 1,
              weightedHit = metrics.weightedHit + req.cost
            )
          case None =>
            // 缓存 miss => 重算并放入
            policy.onPut(req.key, valueProvider(req.key), req.cost, req.tick)
            metrics = metrics.copy(recomputeCost = metrics.recomputeCost + req.cost)
            lastPutTime(req.key) = req.tick
            // 记录依赖（用于后续失效）：
            dependencyIndex.foreach { di =>
              dependencyMap.get(req.key).foreach { deps =>
                deps.foreach(d => di.add(d, req.key))
              }
            }
        }
      case OpType.Invalidate =>
        // 你可以扩展主动 Invalidate 请求
        policy.invalidate(Seq(req.key), req.tick)
    }
  }

  private def handleInvalidation(tick: Long, dep: String): Unit = {
    metrics = metrics.copy(invalidations = metrics.invalidations + 1)
    dependencyIndex.foreach { di =>
      val keys = di.affected(dep).toSeq
      // 统计失效成本（若这些key后续访问需重算）
      val potentialLoss = keys.flatMap { k =>
        // 假设我们能读取其 cost（简单：没缓存则0）
//        None
        Some(0)
      }.sum
      metrics = metrics.copy(invalidationCostLoss = metrics.invalidationCostLoss + potentialLoss)
      policy.invalidate(keys, tick)
    }
  }
}

