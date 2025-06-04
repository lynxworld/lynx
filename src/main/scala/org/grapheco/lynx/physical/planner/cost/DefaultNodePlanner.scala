package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.logical.plans.GraphPatternNode
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Filter, NodesPlanFactory, PhysicalPlan, SingleNodePlan}
import org.grapheco.lynx.runner.IndexManager
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.opencypher.v9_0.expressions.{Ands, Expression, HasLabels, LogicalVariable, Variable}
import org.opencypher.v9_0.util.InputPosition

trait NodePlanner {
  def plan: Seq[PhysicalPlan]
}

case class DefaultNodePlanner(node: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends NodePlanner{
  val GraphPatternNode(variableName, labels, expressions, _) = node

  def plan: Seq[PhysicalPlan] = {
    val factory = NodesPlanFactory(variableName)(plannerContext)
    // 1. scan by label
    val scanLabelPlans = labels.map { label =>
      factory.nodeScanByLabel(label) ~> filter(labels.filterNot(label.eq), expressions)
    }
    //TODO 2.scan by index
    // 3. seek by id

    // Finally. all nodes
    Seq(factory.allNodes ~> filter(labels, expressions)) ++ scanLabelPlans
  }

  def makeFilter: Option[Filter] = filter(labels, expressions)

  private def filter(labels: Seq[LynxNodeLabel], predicates: Seq[Expression]): Option[Filter] = {
    val ip: InputPosition = InputPosition.NONE
    val allExpr = predicates ++ labels.map(label => HasLabels(Variable(variableName)(ip), Seq(label.toNodeLabel))(ip))
    allExpr match {
      case Seq() => None
      case Seq(expr) => Some(Filter(expr))
      case _ => Filter.multi(allExpr)
    }
  }
}
