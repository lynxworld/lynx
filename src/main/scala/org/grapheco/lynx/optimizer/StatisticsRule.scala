package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.plans.{Aggregation, NodeCountFromStatistics, NodeScan, PhysicalPlan, RelationshipCountFromStatistics, RelationshipScan}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.SemanticDirection.BOTH
import org.opencypher.v9_0.expressions.{CountStar, FunctionInvocation, FunctionName, Namespace, NodePattern, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.InputPosition

object StatisticsRule extends PhysicalPlanOptimizerRule{
  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan =
    if(ppc.runnerContext.graphModel.statistics==null) plan
    else optimizeBottomUp(plan, {
      case parent@Aggregation(aggregations, groupings) => {
        if(groupings.isEmpty){
          aggregations.collectFirst {
            case AliasedReturnItem(
            ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("count"),
            false, Vector(Variable(v)))), lv) => (v,lv.name)
            case AliasedReturnItem(
            ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("count"),
            true, Vector(
            ProcedureExpression(FunctionInvocation(Namespace(List()),FunctionName("id"),false,Vector(Variable(v))))
            ))), lv) => (v,lv.name)
          }.map{ case(variable, logicalVariable) =>
            val result = parent.children match {
              case Seq(ns@NodeScan(NodePattern(Some(Variable(vn)),labels,None,None), optional)) if vn==variable =>
                NodeCountFromStatistics(labels.headOption.map(_.name).map(LynxNodeLabel), logicalVariable)(ppc)
              case Seq(ns@NodeScan(NodePattern(Some(Variable(vn)),labels,v1,v2), optional)) if vn==variable =>
                val item = new AliasedReturnItem(CountStar()(InputPosition(0,0,0)), Variable(logicalVariable)(InputPosition(0,0,0)))(InputPosition(0,0,0))
                Aggregation(Seq(item),Seq.empty)(parent.children.head,ppc)
              case Seq(rs@RelationshipScan(
              RelationshipPattern(Some(Variable(vn)), types, None, None, direction, false, None),
              NodePattern(_, Seq(), None, None),
              NodePattern(_, Seq(), None, None), optional)) if (vn==variable && direction!=BOTH)=>
                RelationshipCountFromStatistics(types.headOption.map(_.name).map(LynxRelationshipType), logicalVariable)(ppc)
              case _ => parent
            }
            result
          }.getOrElse(parent)
        }else parent
      }
    })
}
