package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.{PPTAggregation, PPTNode, PPTNodeCountFromStatistics, PPTNodeScan, PPTRelationshipCountFromStatistics, PPTRelationshipScan, PhysicalPlannerContext}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.SemanticDirection.BOTH
import org.opencypher.v9_0.expressions.{FunctionInvocation, FunctionName, Namespace, NodePattern, RelationshipPattern, Variable}

object StatisticsRule extends PhysicalPlanOptimizerRule{
  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode =
    if(ppc.runnerContext.graphModel.statistics==null) plan
    else optimizeBottomUp(plan, {
      case parent@PPTAggregation(aggregations, groupings) => {
        aggregations.collectFirst {
          case AliasedReturnItem(
          ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("count"),
          false, Vector(Variable(v)))), lv) => (v,lv.name)
        }.map{ case(variable, logicalVariable) =>
          parent.children match {
            case Seq(ns@PPTNodeScan(NodePattern(Some(Variable(vn)),labels,None,None))) if vn==variable =>
              PPTNodeCountFromStatistics(labels.headOption.map(_.name).map(LynxNodeLabel), logicalVariable)(ppc)
            case Seq(rs@PPTRelationshipScan(
            RelationshipPattern(Some(Variable(vn)), types, None, None, direction, false, None),
            NodePattern(_, Seq(), None, None),
            NodePattern(_, Seq(), None, None))) if (vn==variable && direction!=BOTH)=>
              PPTRelationshipCountFromStatistics(types.headOption.map(_.name).map(LynxRelationshipType), logicalVariable)(ppc)
            case _ => parent
          }
        }.getOrElse(parent)
    }
  })
}
