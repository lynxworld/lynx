package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{ExecuteException, PhysicalPlannerContext}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxId
import org.grapheco.lynx.types.{LTNode, LynxType}
import org.opencypher.v9_0.expressions.{Expression, LogicalVariable}

case class NodeSeekByID(logicalVariable: Option[LogicalVariable], id: Option[Expression])(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = Seq(logicalVariable.map(_.name).getOrElse("") -> LTNode)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    DataFrame(schema, () => graphModel.nodeAt(id.map(eval(_).asInstanceOf[LynxInteger]).map(i => new LynxId {
      override val value: Any = i.value
      override def toLynxInteger: LynxInteger = i
    }).getOrElse(throw ExecuteException("id of node can not be empty"))).map(Seq(_)).toIterator)
  }
}

