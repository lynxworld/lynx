package org.grapheco.lynx.infer

trait InferEngine {
  def adviser: InferAdviser
}

object NoneInferEngine extends InferEngine {
  override def adviser: InferAdviser = InferAdviser.none
}

object InferEngine {
  def remote: RemoteInferEngine = new RemoteInferEngine

  def none: InferEngine = NoneInferEngine
}
