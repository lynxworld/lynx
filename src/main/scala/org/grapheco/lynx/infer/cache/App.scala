package org.grapheco.lynx.infer.cache

import org.grapheco.lynx.infer.cache.core.CachePolicy
import org.grapheco.lynx.infer.cache.policy.{GDSFCostCache, HPCache, LFUCache, LRUCache, RandomCache}
import org.grapheco.lynx.infer.cache.sim.{DependencyIndex, Simulator, TraceGenerator}
import org.grapheco.lynx.runner.infercache.core._
import org.grapheco.lynx.runner.infercache.policy._
import org.grapheco.lynx.runner.infercache.sim._

import scala.util.Random

object App extends App {

  val rnd = new Random(42)

  // 生成一个两阶段工作负载：前半 50k 次访问 key 集=200，后半换 200 新 key
  val trace = TraceGenerator.phaseShift(
    phaseLen = 50000,
    keysA = 2000,
    keysB = 2000,
    thetaA = 0.9,
    thetaB = 1.1,
    baseCostA = i => (5 + (i % 20)).toLong,   // 前半成本
    baseCostB = i => (50 + (i % 150)).toLong, // 后半成本更高
    rnd
  )

  // 注入失效事件（示意）
  val deps = (1 to 50).map(i => s"dep$i")
  val traceWithInv = TraceGenerator.injectInvalidations(trace, deps, interval = 10000, affectedFraction = 0.1, rnd)

  // valueProvider：模拟值对象
  def valueProvider(k: String): String = s"VAL:$k"

  // 构造策略列表
  val capacity = 500
  val policies: Seq[CachePolicy[String, String]] = Seq(
    new LRUCache[String, String](capacity),
    new LFUCache[String, String](capacity),
    new RandomCache[String, String](capacity),
    new HPCache[String, String](capacity, alpha = 1.0, beta = 1.0, gamma = 0.5),
    new GDSFCostCache[String, String](capacity),
//    new TinyLFUSimpleCache[String, String](capacity, windowFraction = 0.2)
  )

  // 依赖映射（示例：每个 key 依赖一个 dep）
  val keyDeps: Map[String, List[String]] =
    (0 until 400).map { i =>
      val k = s"k$i"
      val dep = deps(i % deps.size)
      k -> List(dep)
    }.toMap

  policies.foreach { p =>
    val depIndex = new DependencyIndex[String]()
    val sim = new Simulator[String, String](p, valueProvider, Some(depIndex))
    val result = sim.run(traceWithInv, keyDeps)
    println(s"=== Policy: ${result.policyName} ===")
    println(f"HitRatio=${result.metrics.hitRatio}%.4f  WeightedHitRatio=${result.metrics.weightedHitRatio}%.4f  CostSaving=${result.metrics.costSaving}%.4f")
    println(s"TotalReq=${result.metrics.totalReq} Hits=${result.metrics.hits} RecomputeCost=${result.metrics.recomputeCost}")
    println(s"Invalidations=${result.metrics.invalidations}  InvalidationCostLoss=${result.metrics.invalidationCostLoss}")
    println("Extra: " + result.extra)
    println()
  }
}

