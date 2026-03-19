package org.grapheco.lynx.infer.cache.core

// 统一指标
case class CacheMetrics(
                         gets: Long = 0L,
                         hits: Long = 0L,
                         computeCost: Double = 0.0,
                         hitCost: Double = 0.0,
                         invalidations: Long = 0L,
                         evictions: Long = 0L,
                         evictionsCost: Long = 0L,
                       ) {

  def hitRatio: Double = if (gets == 0) 0.0 else hits.toDouble / gets

  def weightedHitRatio: Double = {
    val denom = hitCost + computeCost
    if (denom == 0) 0.0 else hitCost / denom
  }

  override def toString: String =
    s"""
       |gets: $gets,
       |hits: $hits,
       |invalidations: $invalidations,
       |evictions: $evictions,
       |hitRatio: $hitRatio,
       |weightedHitRatio: $weightedHitRatio,
       |computeCost: $computeCost,
       |costSaving: $hitCost,
       |evictionsCost: $evictionsCost
       |""".stripMargin
}
