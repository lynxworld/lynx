package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.logical.plans.GraphPatternNode
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Filter, InferFakeNode, InferPlanner, NodesPlanFactory, PhysicalPlan, VNodeFromList}
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.opencypher.v9_0.expressions.{Expression, HasLabels, In, Variable}
import org.opencypher.v9_0.util.InputPosition

trait VNodePlanner {
  def plan: Seq[PhysicalPlan]
}

case class DefaultVNodePlanner(node: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends VNodePlanner{
  val GraphPatternNode(variableName, labels, expressions, virtual, _) = node
  implicit val v2s: Variable => String = _.name
  def plan: Seq[PhysicalPlan] = {
    var plans: Seq[PhysicalPlan] = Seq.empty
    // first: fake
    plans ++= Seq(InferFakeNode(node))
    val inExp = node.expressions.collectFirst{case i: In => i}

    inExp.foreach { in =>
      val others = node.expressions.toSet.-(in).toSeq
      val listVariableName = in.dependencies.map(_.name).-(node.variableName).head

      plans ++=
        InferPlanner.makeFilters(node.copy(expressions=others))(VNodeFromList(node.copy(labels = Seq.empty, expressions = Seq.empty), listVariableName))

    }
    plans
  }

}