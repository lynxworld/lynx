package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.logical.plans.LogicalPatternMatch
import org.grapheco.lynx.physical
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{FromArgument, PPTExpandPath, PPTNodeScan, PPTRelationshipScan, PhysicalPlan}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

// TODO: very complex, need more think!
case class PPTPatternMatchTranslator(patternMatch: LogicalPatternMatch)(implicit val plannerContext: PhysicalPlannerContext) extends PPTNodeTranslator {
  private def planPatternMatch(pm: LogicalPatternMatch)(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    // TODO: if arguments is not the first variable? these need moved to Optimizer!
    val argumentHit = ppc.argumentContext.contains(patternMatch.headNode.variable.map(_.name).getOrElse(""))
    val LogicalPatternMatch(optional, variableName, headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) = pm
    if (argumentHit) {
      chain.toList match {
        //match (m)
        case Nil => FromArgument(headNode.variable.get.name)(ppc)
        //match (m)-[r]-(n)
        //        case List(Tuple2(rel, rightNode)) => PPTRelationshipScan(rel, headNode, rightNode)(ppc)
        //match (m)-[r]-(n)-...-[p]-(z)
        case _ =>
          val (lastRelationship, lastNode) = chain.last
          val dropped = chain.dropRight(1)
          val part = planPatternMatch(LogicalPatternMatch(optional, variableName, headNode, dropped))(ppc)
          PPTExpandPath(lastRelationship, lastNode)(part, plannerContext)
      }
    } else {
      chain.toList match {
        //match (m)
        case Nil => PPTNodeScan(headNode)(ppc)
        //match (m)-[r]-(n)
        case List(Tuple2(rel, rightNode)) => PPTRelationshipScan(rel, headNode, rightNode)(ppc)
        //match (m)-[r]-(n)-...-[p]-(z)
        case _ =>
          val (lastRelationship, lastNode) = chain.last
          val dropped = chain.dropRight(1)
          val part = planPatternMatch(LogicalPatternMatch(optional, variableName, headNode, dropped))(ppc)
          physical.plans.PPTExpandPath(lastRelationship, lastNode)(part, plannerContext)
      }
    }
  }

  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    planPatternMatch(patternMatch)(ppc)
  }
}
