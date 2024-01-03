package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.CTNode

case class PPTNodeScan(pattern: NodePattern)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern
    Seq(var0.name -> CTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern
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
    DataFrame(Seq(var0.name -> CTNode), () => {
      graphModel.nodes(
        NodeFilter(
          labels.map(_.name).map(LynxNodeLabel),
          nodeProperties, nodeProps
        )
      ).map(Seq(_))
    })
  }
}
