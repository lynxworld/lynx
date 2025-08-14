package org.grapheco.lynx.runner.infer

import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship}

sealed trait InferExecutor

abstract class InferExpandExecutor extends InferExecutor {
  def infer(node: LynxNode): Seq[(LynxRelationship, LynxNode)]
}

abstract class InferPropertyExecutor extends InferExecutor {
  def infer(node: LynxNode, props: Seq[LynxPropertyKey] = Seq.empty): LynxNode
}

abstract class InferLabelExecutor extends InferExecutor {
  def infer(node: LynxNode): LynxNode
}

abstract class InferLinkExecutor extends InferExecutor {
  def infer(node: LynxNode, nodes: Seq[LynxNode]): Seq[(LynxNode, LynxRelationship, LynxNode)]
}