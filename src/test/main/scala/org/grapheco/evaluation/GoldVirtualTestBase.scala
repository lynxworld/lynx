package org.grapheco.evaluation

import org.grapheco.lynx.runner.CypherRunner
import org.grapheco.lynx.runner.infer.{Condition, InferAdviser, InferEngine, RemoteInferAdviser}

class GoldVirtualTestBase extends VirtualTestBase {
  override val runner: CypherRunner = new CypherRunner(graphModel = model) {
    val adviser: InferAdviser = RemoteInferAdviser()
      .addInfer(Condition(relType = Seq("contains")), GoldContainsInfer)
      .addInfer(Condition(), GoldLabel)
      .addInfers(
        Seq("front", "behind", "left", "right")
          .map(relType => Condition(relType = Seq(relType)) -> GoldLink).toMap
      )
      .addInfers(
        Seq("color", "material", "size")
          .map(prop => Condition(nodeProps = Seq(prop)) -> GoldProps).toMap
      )

    override protected lazy val inferEngine: InferEngine = InferEngine.remote.withAdviser(adviser)
  }


}
