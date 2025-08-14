package org.grapheco.evaluation

import org.grapheco.lynx.runner.CypherRunner
import org.grapheco.lynx.runner.infer._

class VirtualTestBase extends TestBase {
  override val runner: CypherRunner = new CypherRunner(graphModel = model) {
    val adviser: InferAdviser = RemoteInferAdviser()
      .addInfer(Condition(relType = Seq("contains")), ContainsInfer)
      .addInfer(Condition(nodeProps = Seq("color")), ColorInfer)
      .addInfer(Condition(nodeProps = Seq("material")), MaterialInfer)
      .addInfer(Condition(), ShapeInfer)
      .addInfers(
        Seq("front", "behind", "left", "right")
          .map(relType => Condition(relType = Seq(relType)) -> PositionInfer).toMap
      )

    override protected lazy val inferEngine: InferEngine = InferEngine.remote.withAdviser(adviser)
  }


}


