package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.physical.plans.PhysicalPlan
import org.opencypher.v9_0.expressions.Expression

case class Candidate(plan: PhysicalPlan,
                     cardinal: Long,
                     cost: Double = 0,
                     filters: Seq[Expression] = Seq.empty) {
  def withFilters(filters: Seq[Expression]): Candidate = this.copy(filters = filters)
}

object Candidate {
  def apply(plan: PhysicalPlan): Candidate = Candidate(plan, 0)
}
