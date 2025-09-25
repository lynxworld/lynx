package org.grapheco.lynx.infer.cache.sim

import org.grapheco.lynx.infer.cache.core.{OpType, Request}

import scala.util.Random

object TraceGenerator {

  // Zipf 分布采样
  def zipfKeys(n: Int, uniqueKeys: Int, theta: Double, baseCost: Int => Long, rnd: Random): Seq[Request[String]] = {
    val ranks = (1 to uniqueKeys).map(_.toDouble)
    val weights = ranks.map(r => 1.0 / math.pow(r, theta))
    val sum = weights.sum
    val probs = weights.map(_ / sum)

    val cdf = probs.scanLeft(0.0)(_ + _).tail
    def sampleKey(): Int = {
      val x = rnd.nextDouble()
      var i = 0
      while (i < cdf.length && cdf(i) < x) i += 1
      i
    }

    (0 until n).map { i =>
      val kIdx = sampleKey()
      val key = s"k$kIdx"
      val cost = baseCost(kIdx)
      Request(i.toLong, key, OpType.Get, cost)
    }
  }

  // 混合阶段：前半使用集合A，后半集合B
  def phaseShift(
                  phaseLen: Int,
                  keysA: Int,
                  keysB: Int,
                  thetaA: Double,
                  thetaB: Double,
                  baseCostA: Int => Long,
                  baseCostB: Int => Long,
                  rnd: Random
                ): Seq[Request[String]] = {
    val part1 = zipfKeys(phaseLen, keysA, thetaA, baseCostA, rnd)
    val part2 = zipfKeys(phaseLen, keysB, thetaB, baseCostB, rnd).map(r => r.copy(tick = r.tick + phaseLen))
    part1 ++ part2
  }

  // 插入失效事件（简单：每 interval 按概率选若干 keys 作为“依赖”失效）
  def injectInvalidations[K](
                              trace: Seq[Request[K]],
                              dependencyUniverse: Seq[String],
                              interval: Int,
                              affectedFraction: Double,
                              rnd: Random
                            ): Seq[Either[Request[K], (Long, String)]] = {
    val depCount = dependencyUniverse.size
    val buf = scala.collection.mutable.ArrayBuffer[Either[Request[K], (Long, String)]]()
    trace.foreach { r =>
      buf += Left(r)
      if (r.tick % interval == 0 && r.tick > 0) {
        val dep = dependencyUniverse(rnd.nextInt(depCount))
        buf += Right(r.tick -> dep)
      }
    }
    buf.toSeq
  }
}

