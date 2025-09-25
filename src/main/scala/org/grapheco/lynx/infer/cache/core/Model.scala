package org.grapheco.lynx.infer.cache.core

sealed trait OpType
object OpType {
  case object Get extends OpType
  case object Invalidate extends OpType
  case object Put extends OpType // 预留
}

case class Request[K](
                       tick: Long,          // 时间步序号（模拟用）
                       key: K,
                       op: OpType,
                       cost: Long,          // 重算成本（用于加权命中）
                       dependencyIds: List[String] = Nil // 访问涉及的依赖（用于统计/未来特征）
                     )

case class InvalidationEvent(
                              tick: Long,
                              affectedKeys: List[String] // 这里假定 key 可转 string；真实可用 K
                            )

// 统一指标
case class CacheMetrics(
                    totalReq: Long = 0L,
                    gets: Long = 0L,
                    hits: Long = 0L,
                    weightedHit: Double = 0.0,
                    recomputeCost: Double = 0.0,
                    hitCost: Double = 0.0,
                    invalidations: Long = 0L,
                    invalidationCostLoss: Double = 0.0,   // 因失效导致的额外重算成本
                    evictions: Long = 0L,
                    prematureEvictions: Long = 0L,        // 被驱逐后很快又访问
                    recordWindow: Long = 10000L          // 用来判断“很快”阈值（请求步差）
                  ) {
  def hitRatio: Double = if (gets == 0) 0.0 else hits.toDouble / gets
  def weightedHitRatio: Double = {
    val denom = weightedHit + recomputeCost
    if (denom == 0) 0.0 else weightedHit / denom
  }
  def costSaving: Double = {
    val base = weightedHit + recomputeCost
    if (base == 0) 0.0 else weightedHit / base
  }

  override def toString: String =
    s"""
       |totalReq: $totalReq,
       |gets: $gets,
       |hits: $hits,
       |weightedHit: $weightedHit,
       |recomputeCost: $recomputeCost,
       |invalidations: $invalidations,
       |invalidationCostLoss: $invalidationCostLoss,
       |evictions: $evictions,
       |prematureEvictions: $prematureEvictions,
       |recordWindow: $recordWindow,
       |hitRatio: $hitRatio,
       |weightedHitRatio: $weightedHitRatio,
       |costSaving: $costSaving
       |""".stripMargin
}
