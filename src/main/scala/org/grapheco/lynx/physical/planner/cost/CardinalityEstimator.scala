package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.physical.plans.{AllNode, AllRelationships, Expand2, Filter, InferExpand, InferLabel, InferProperties, NodeScanByLabel2, PhysicalPlan, RelationshipsByType, SinglePhysicalPlan}
import org.grapheco.lynx.runner.GraphModel
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}


trait CardinalityEstimator {
  def nodeCardinalityAll: Long
  def nodeCardinality(label: LynxNodeLabel): Long
  def relCardinalityAll: Long
  def relCardinality(typo: LynxRelationshipType): Long
  def estimate(plan: PhysicalPlan)(inCard: Seq[Long] = Seq.empty): Long
}

trait CacheCardinalityEstimator extends CardinalityEstimator {

}

abstract class AbstractCardinalityEstimator extends CardinalityEstimator {
  override def estimate(plan: PhysicalPlan)(inCard: Seq[Long] = Seq.empty): Long = plan match {
    case an: AllNode => nodeCardinalityAll
    case ns: NodeScanByLabel2 => nodeCardinality(ns.labelName)
    case fl: Filter => inCard.head// selective
    case ex: Expand2 => inCard.head
    case ar: AllRelationships => relCardinalityAll
    case rt: RelationshipsByType => relCardinality(rt.typeName)
    case ie: InferExpand => inCard.head
    case ip: InferProperties => inCard.head
    case il: InferLabel => inCard.head
    case s: SinglePhysicalPlan => inCard.head
    case _ => throw new RuntimeException("Unsupported plan type: " + plan.getClass.getSimpleName)
  }
}

class ExactlyCardinalityEstimator(graphModel: GraphModel) extends AbstractCardinalityEstimator {
  override def nodeCardinalityAll: Long = graphModel.nodes().size
  override def relCardinalityAll: Long = graphModel.relationships().size
  override def relCardinality(typo: LynxRelationshipType): Long = graphModel.relationships().map(_.storedRelation.relationType).count(_.contains(typo))
  override def nodeCardinality(label: LynxNodeLabel): Long = graphModel.nodes().map(_.labels).count(_.contains(label))
}
