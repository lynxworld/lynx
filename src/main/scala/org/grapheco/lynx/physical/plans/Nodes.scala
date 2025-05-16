package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.logical.plans.GraphPatternNode
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions._

sealed abstract class NodesPlan( variable: String) extends LeafPhysicalPlan {
  override def schema: Seq[(String, LynxType)] = Seq(variable -> LTNode)
}

case class NodesPlanFactory(variable: String)(implicit val plannerContext: PhysicalPlannerContext){
  def allNodes(): AllNodes = AllNodes()(variable)

  def nodeScan(pattern: GraphPatternNode): NodeScanByLabel = NodeScanByLabel(pattern)(variable)

  def seekByIndex(pattern: GraphPatternNode): NodeSeekByIndex = NodeSeekByIndex(pattern)(variable)

  def seekByID(expr: Expression): NodeSeekByID = NodeSeekByID(expr)(variable)

}

/**
 * Scan All Nodes
 */
case class AllNodes()(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends NodesPlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = DataFrame(schema, () => graphModel.nodes().map(Seq(_)))
}

/**
 * Scan Nodes By Labels
 */
case class NodeScanByLabel(pattern: GraphPatternNode)(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends NodesPlan(variable) {

  override def schema: Seq[(String, LynxType)] = {
    val GraphPatternNode(
    Some(var0: String),
    labels: Seq[LabelName],
    properties: Option[Expression],
    optional) = pattern
    Seq(var0 -> LTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val GraphPatternNode(
    Some(var0: String),
    labels: Seq[LynxNodeLabel],
    properties: Option[Expression],
    optional) = pattern
    implicit val ec = ctx.expressionContext

    val (nodeProperties, nodeProps) = if (properties.isEmpty) (Map.empty[LynxPropertyKey, LynxValue], Map.empty[LynxPropertyKey, PropOp])
    else properties.get match {
      case li@ListLiteral(expressions) => {
        (eval(expressions(0)).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))
          , eval(expressions(1)).asInstanceOf[LynxMap].value.map(kv => {
          val v_2: PropOp = kv._2.value.toString match {
            case "IN" => IN
            case "EQUAL" => EQUAL
            case "NOTEQUALS" => NOT_EQUAL
            case "LessThan" => LESS_THAN
            case "LessThanOrEqual" => LESS_THAN_OR_EQUAL
            case "GreaterThan" => GREATER_THAN
            case "GreaterThanOrEqual" => GREATER_THAN_OR_EQUAL
            case "Contains" => CONTAINS
            case _ => throw new scala.Exception("unexpected PropOp" + kv._2.value)
          }
          (LynxPropertyKey(kv._1), v_2)
        }))
      }
      case _ => {
        (properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty), Map.empty[LynxPropertyKey, PropOp])
      }
    }
    DataFrame(Seq(var0 -> LTNode), () => {
      graphModel.nodes(
        NodeFilter(
          labels, nodeProperties, nodeProps
        )
      ).map(Seq(_))
    })
  }
}

/**
 * Seek Node By Index
 */
case class NodeSeekByIndex(pattern: GraphPatternNode)(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends NodesPlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = ???
}

/**
 * Seek Node By Id
 * @param expr Id expression
 */
case class NodeSeekByID(expr: Expression)(variable: String)(implicit val plannerContext: PhysicalPlannerContext) extends NodesPlan(variable){
  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec: ExpressionContext = ctx.expressionContext
    DataFrame(schema, () => graphModel.nodeAt(new LynxId {
      val id: LynxInteger = eval(expr).asInstanceOf[LynxInteger]
      override val value: Any = id
      override def toLynxInteger: LynxInteger = id
    }).map(Seq(_)).map(Iterator(_)).getOrElse(throw ExecuteException("id of node can not be empty")))
  }
}
