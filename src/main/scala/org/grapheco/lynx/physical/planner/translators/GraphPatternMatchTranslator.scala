package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.logical.plans.{GraphPatternMatch, LogicalPatternMatch}
import org.grapheco.lynx.physical
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{Expand, FromArgument, PhysicalPlan, RelationshipScan}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.planner.cost.{CardinalityEstimator, CostBasedPlanner, CostCalculator, DefaultCostCalculator, ExactlyCardinalityEstimator}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxType
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

case class GraphPatternMatchTranslator(patternMatch: GraphPatternMatch)(implicit val plannerContext: PhysicalPlannerContext) extends PPTNodeTranslator {

  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    val cardinalityEstimator: CardinalityEstimator = new ExactlyCardinalityEstimator(ppc.runnerContext.graphModel)
    val costCalculator: CostCalculator = new DefaultCostCalculator(cardinalityEstimator)
    val planner = new CostBasedPlanner(costCalculator)
    planner.plan(patternMatch, in)(ppc)
  }

}
