package org.grapheco.lynx.infer

import org.grapheco.lynx.LynxException

abstract class InferAdviser {
  def forExpand(c: Condition): Option[InferExpandExecutor]

  def forLabel(c: Condition): Option[InferLabelExecutor]

  def forProperty(c: Condition): Option[InferPropertyExecutor]

  def forLink(c: Condition): Option[InferLinkExecutor]
}

object InferAdviser {
  def none: InferAdviser = new InferAdviser {
    override def forExpand(c: Condition): Option[InferExpandExecutor] = None
    override def forLabel(c: Condition): Option[InferLabelExecutor] = None
    override def forProperty(c: Condition): Option[InferPropertyExecutor] = None
    override def forLink(c: Condition): Option[InferLinkExecutor] = None
  }
}


case class Condition(
                    val nodeLabel: Seq[String] = Seq.empty,
                    val nodeProps: Seq[String] = Seq.empty,
                    val relType: Seq[String] = Seq.empty,
                    val rightLabel: Seq[String] = Seq.empty,
                    ) {

  def isMatch(c: Condition): Boolean = {
    nodeLabel.forall(c.nodeLabel.contains) &&
      nodeProps.forall(c.nodeProps.contains) &&
      relType.forall(c.relType.contains) &&
      rightLabel.forall(c.rightLabel.contains)
  }
}

case class NotMatchInferExecutorFoundException(s: String) extends LynxException {
  override def getMessage: String = "Not match infer executor found: "+s
}
