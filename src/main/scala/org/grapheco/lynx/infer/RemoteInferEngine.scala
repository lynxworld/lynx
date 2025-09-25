package org.grapheco.lynx.infer

class RemoteInferEngine(val inferAdviser: InferAdviser = InferAdviser.none) extends InferEngine {
  override def adviser: InferAdviser = inferAdviser

  def withAdviser(adviser: InferAdviser): RemoteInferEngine = {
    new RemoteInferEngine(adviser)
  }
}

case class RemoteInferAdviser(allInfer: Map[Condition, InferExecutor] = Map.empty) extends InferAdviser {

  override def forExpand(c: Condition): Option[InferExpandExecutor] =
    allInfer.filter(_._1.isMatch(c)).values.collectFirst{case i:InferExpandExecutor => i}

  override def forLabel(c: Condition): Option[InferLabelExecutor] =
    allInfer.filter(_._1.isMatch(c)).values.collectFirst{case i:InferLabelExecutor => i}

  override def forProperty(c: Condition): Option[InferPropertyExecutor] =
    allInfer.filter(_._1.isMatch(c)).values.collectFirst{case i:InferPropertyExecutor => i}

  override def forLink(c: Condition): Option[InferLinkExecutor] =
    allInfer.filter(_._1.isMatch(c)).values.collectFirst{case i:InferLinkExecutor => i}

  def addInfer(c: Condition, infer: InferExecutor): RemoteInferAdviser = {
    RemoteInferAdviser(allInfer + (c -> infer))
  }

  def addInfers(infers: Map[Condition, InferExecutor]): RemoteInferAdviser = {
    RemoteInferAdviser(allInfer ++ infers)
  }
}
