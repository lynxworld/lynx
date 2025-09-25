package org.grapheco.evaluation

import org.grapheco.lynx.infer.cache.core.{OpType, Request}

import scala.util.Random

object ZipfWorkloadBuilder {

  def zipfProbs(n: Int, theta: Double): Array[Double] = {
    val w = Array.tabulate(n)(i => 1.0 / math.pow(i + 1, theta))
    val s = w.sum
    w.map(_ / s)
  }

  // 前缀累积 -> 二分采样（n=1000 足够快）
  def sampleIndex(cdf: Array[Double], rnd: Random): Int = {
    val x = rnd.nextDouble()
    var lo = 0; var hi = cdf.length - 1
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      if (cdf(mid) >= x) hi = mid else lo = mid + 1
    }
    lo
  }

  def dependencyOut(i: Int): Int = {
    if (i < 50) 0
    else if (i < 150) 5
    else if (i < 350) 8
    else 1
  }

  def buildZipfImageLevel(
                           numImages: Int = 1000,
                           totalRequests: Int = 10000,
                           theta: Double = 0.9,
                           rnd: Random = new Random(42),
                         ): Seq[Request[(Int, Int)]] = {

    val probs = zipfProbs(numImages, theta)
    val cdf = probs.scanLeft(0.0)(_ + _).tail

    (0 until totalRequests).map { t =>
      val img = sampleIndex(cdf, rnd)
      val k = (img, Random.nextInt(10))
      Request[(Int, Int)](
        tick = t.toLong,
        key = k,
        op = OpType.Get,
        cost = 0,
        dependencyIds = Nil
      )
    }
  }



}
