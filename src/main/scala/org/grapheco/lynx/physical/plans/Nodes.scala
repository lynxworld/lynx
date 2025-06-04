package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.logical.plans.GraphPatternNode
import org.grapheco.lynx.physical.planner.cost.NodePlanner
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions._

sealed abstract class SingleNodePlan(variable: String) extends LeafPhysicalPlan {
  override def schema: Seq[(String, LynxType)] = Seq(variable -> LTNode)
}

case class NodesPlanFactory(variable: String)(implicit val plannerContext: PhysicalPlannerContext){

  def allNodes: AllNode = AllNode(variable)

  def nodeScanByLabel(lynxNodeLabel: LynxNodeLabel): NodeScanByLabel = NodeScanByLabel(lynxNodeLabel, variable)

//  def seekByIndex: NodeSeekByIndex = NodeSeekByIndex(patternNode)(variable)

  def nodeSeekByID(expression: Expression): NodeSeekByID = NodeSeekByID(expression, variable)

}

/**
 * Scan All Nodes
 */
case class AllNode(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends SingleNodePlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = DataFrame(schema, () => graphModel.nodes().map(Seq(_)))

  override def toString: String = s"AllNode($variable)"
}

case class NodeScanByLabel(labelName: LynxNodeLabel, variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends SingleNodePlan(variable) {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame(schema, () => graphModel.nodes(NodeFilter(Seq(labelName), Map.empty, Map.empty)).map(Seq(_)))
  }

  override def toString: String = s"NodeScanByLabel($variable:$labelName)"

}

/**
 * Seek Node By Index
 */
case class NodeSeekByIndex(pattern: GraphPatternNode)(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends SingleNodePlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = ???
}

/**
 * Seek Node By Id
 * @param expr Id expression
 */
case class NodeSeekByID(expr: Expression, variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends SingleNodePlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec: ExpressionContext = ctx.expressionContext
    DataFrame(schema, () => graphModel.nodeAt(new LynxId {
      val id: LynxInteger = eval(expr).asInstanceOf[LynxInteger]
      override val value: Any = id
      override def toLynxInteger: LynxInteger = id
    }).iterator.map(Seq(_)))
  }
}
