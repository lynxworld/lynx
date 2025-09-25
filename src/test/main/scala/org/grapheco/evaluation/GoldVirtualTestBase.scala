package org.grapheco.evaluation

import org.grapheco.lynx.infer.{Condition, InferAdviser, InferEngine, RemoteInferAdviser}
import org.grapheco.lynx.runner.CypherRunner

class GoldVirtualTestBase extends VirtualTestBase {
  override val runner: CypherRunner = new CypherRunner(graphModel = model) {
    val adviser: InferAdviser = RemoteInferAdviser()
      .addInfer(Condition(relType = Seq("contains")), GoldExpand)
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
