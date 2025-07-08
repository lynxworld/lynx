package org.grapheco.lynx.runner.infer

import org.grapheco.lynx.LynxException

abstract class InferAdviser {
  def forExpand(c: Condition): Option[InferExpandExecutor]

  def forLabel(c: Condition): Option[InferLabelExecutor]

  def forProperty(c: Condition): Option[InferPropertyExecutor]
}

object InferAdviser {
  def none: InferAdviser = new InferAdviser {
    override def forExpand(c: Condition): Option[InferExpandExecutor] = None
    override def forLabel(c: Condition): Option[InferLabelExecutor] = None
    override def forProperty(c: Condition): Option[InferPropertyExecutor] = None
  }
}


case class Condition(
                    val nodeLabel: Seq[String] = Seq.empty,
                    val nodeProps: Seq[String] = Seq.empty,
                    val relType: Seq[String] = Seq.empty,
                    ) {

  def isMatch(c: Condition): Boolean = {
    nodeLabel.forall(c.nodeLabel.contains) &&
      nodeProps.forall(c.nodeProps.contains) &&
      relType.forall(c.relType.contains)
  }
}

case class NotMatchInferExecutorFoundException(s: String) extends LynxException {
  override def getMessage: String = "Not match infer executor found: "+s
}
