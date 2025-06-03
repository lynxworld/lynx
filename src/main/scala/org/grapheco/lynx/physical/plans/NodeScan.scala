package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions._

case class NodeScan(pattern: NodePattern, optional: Boolean = false)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern
    Seq(var0.name -> LTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern
    val ec = ctx.expressionContext
    val df = ctx.arguments
    val newSchema = df.schema.filter(_._1!=var0.name) ++ Seq(var0.name -> LTNode)

    DataFrame(newSchema, () => {
      if(df.schema.size!=0){
        df.records.flatMap(record => {
          val recordCtx = ec.withVars(df.columnsName.zip(record).toMap)
          val filterExpr = getNodeFilerProperties(properties, recordCtx)
          val iter = graphModel.nodes(
            NodeFilter(
              labels.map(_.name).map(LynxNodeLabel),
              Map.empty, filterExpr
            )
          ).map(Seq(_)).map(df.columnsName.zip(record).filter(_._1!=var0.name).map(_._2) ++ _)
          val recordToAdd = if(df.columnsName.contains(var0.name)) df.columnsName.zip(record).filter(_._1==var0.name).map(_._2) else Seq()
          if(iter.isEmpty && optional)  Seq(df.columnsName.zip(record).filter(_._1!=var0.name).map(_._2) ++ recordToAdd) else iter
        })
      }else{
        val filterExpr = getNodeFilerProperties(properties, ec)
        graphModel.nodes(
          NodeFilter(
            labels.map(_.name).map(LynxNodeLabel),
            Map.empty, filterExpr
          )
        ).map(Seq(_))
      }
    })
  }
}
