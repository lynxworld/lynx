package org.grapheco.evaluation

import org.grapheco.lynx.infer.cache.{CacheKey, CacheValue, DefaultInferCache, InferCache}
import org.grapheco.lynx.infer.cache.core.{CacheMetrics, CachePolicy}
import org.grapheco.lynx.infer.cache.depgraph.DepGraph
import org.grapheco.lynx.infer.{Condition, InferAdviser, InferEngine, RemoteInferAdviser}
import org.grapheco.lynx.runner.CypherRunner

class GoldVirtualCacheTestBase(val cache: CachePolicy[CacheKey, CacheValue]) extends GoldVirtualTestBase {

  val dependencyGraph: DepGraph = DepGraph.empty

  implicit val _inferCache: InferCache =  new DefaultInferCache(cache, dependencyGraph)

  override val runner: CypherRunner = new CypherRunner(graphModel = model) {
    val adviser: InferAdviser = RemoteInferAdviser()
      .addInfer(Condition(relType = Seq("contains")), new GoldExpandCache)
      .addInfer(Condition(), new GoldLabelCache)
      .addInfers( // TODO
        Seq("front", "behind", "left", "right")
          .map(relType => Condition(relType = Seq(relType)) -> GoldLink).toMap
      )
      .addInfers(
        Seq("color", "material", "size")
          .map(prop => Condition(nodeProps = Seq(prop)) -> new GoldPropsCache).toMap
      )

    override protected lazy val inferCache: InferCache = _inferCache

    override protected lazy val inferEngine: InferEngine = InferEngine.remote.withAdviser(adviser)

  }
}
