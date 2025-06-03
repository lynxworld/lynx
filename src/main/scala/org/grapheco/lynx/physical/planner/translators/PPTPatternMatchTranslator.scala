package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.logical.plans.LogicalPatternMatch
import org.grapheco.lynx.physical
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{FromArgument, Expand, NodeScan, RelationshipScan, PhysicalPlan}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

// TODO: very complex, need more think!
case class PPTPatternMatchTranslator(patternMatch: LogicalPatternMatch)(implicit val plannerContext: PhysicalPlannerContext) extends PPTNodeTranslator {
  private def planPatternMatch(pm: LogicalPatternMatch)(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    // TODO: if arguments is not the first variable? these need moved to Optimizer!
    //    val argumentHit = ppc.argumentContext.contains(patternMatch.headNode.variable.map(_.name).getOrElse(""))
    val argumentHit = false
    val LogicalPatternMatch(optional, variableName, headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) = pm
    if (argumentHit) {
      chain.toList match {
        //match (m)
        case Nil => FromArgument(Seq(headNode.variable.get.name))(ppc)
        //match (m)-[r]-(n)
        //        case List(Tuple2(rel, rightNode)) => RelationshipScan(rel, headNode, rightNode)(ppc)
        case List(Tuple2(rel, rightNode)) => Expand(rel, rightNode, optional)(FromArgument(Seq(headNode.variable.get.name))(ppc), plannerContext)
        //match (m)-[r]-(n)-...-[p]-(z)
        case _ =>
          val (lastRelationship, lastNode) = chain.last
          val dropped = chain.dropRight(1)
          val part = planPatternMatch(LogicalPatternMatch(optional, variableName, headNode, dropped))(ppc)
          Expand(lastRelationship, lastNode, optional)(part, plannerContext)
      }
    } else {
      chain.toList match {
        //match (m)
        case Nil => NodeScan(headNode, optional)(ppc)
        //match (m)-[r]-(n)
        case List(Tuple2(rel, rightNode)) => RelationshipScan(rel, headNode, rightNode, optional)(ppc)
        //match (m)-[r]-(n)-...-[p]-(z)
        case _ =>
          val (lastRelationship, lastNode) = chain.last
          val dropped = chain.dropRight(1)
          val part = planPatternMatch(LogicalPatternMatch(optional, variableName, headNode, dropped))(ppc)
          Expand(lastRelationship, lastNode, optional)(part, plannerContext)
      }
    }
  }

  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    planPatternMatch(patternMatch)(ppc)
  }
}
