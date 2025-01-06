package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical._
import org.grapheco.lynx.physical.plans.{Join, Merge, PhysicalPlan, NodeScan, RelationshipScan}
import org.grapheco.lynx.runner.GraphModel
import org.opencypher.v9_0.expressions.{Literal, MapExpression, NodePattern, RelationshipPattern}

import scala.collection.mutable

/**
 * rule to estimate two tables' size in PPTJoin
 * mark the bigger table's position
 */
object JoinTableSizeEstimateRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan,
    {
      case pnode: PhysicalPlan if isJoinNode(pnode) => {
        val joinedPlan = joinRecursion(pnode.asInstanceOf[Join], ppc, getIsSingleMatch(pnode))
        pnode.withChildren(Seq(joinedPlan))
      }
      case pnode => pnode
    }
  )

  private def isJoinNode(plan: PhysicalPlan): Boolean = plan match {
    case Join(_, _, _) => true
    case _ => false
  }

  private def getIsSingleMatch(plan: PhysicalPlan): Boolean = plan match {
    case Join(_, isSingleMatch, _) => isSingleMatch
    case _ => false
  }

  def estimateNodeRow(pattern: NodePattern, graphModel: GraphModel): Long = {
    val labels = pattern.labels.map(_.name)
    val prop = pattern.properties.collect {
      case MapExpression(items) => items.map {
        case p if p._2.isInstanceOf[Literal] => (p._1.name, p._2.value)
        case _ => (p._1.name, null)
      }
    }.flatten

    if (labels.nonEmpty) {
      val minLabelAndCount = labels.map(label => (label, graphModel._helper.estimateNodeLabel(label))).minBy(_._2)
      val estimatedPropCount = if (prop.nonEmpty) prop.map(f => graphModel._helper.estimateNodeProperty(minLabelAndCount._1, f._1, f._2)).min else minLabelAndCount._2
      estimatedPropCount
    } else graphModel.statistics.numNode
  }

  def estimateRelationshipRow(rel: RelationshipPattern, left: NodePattern, right: NodePattern, graphModel: GraphModel): Long = {
    if (rel.types.isEmpty) graphModel.statistics.numRelationship
    else graphModel._helper.estimateRelationship(rel.types.head.name)
  }

  def estimate(table: PhysicalPlan, ppc: PhysicalPlannerContext): Long = {
    table match {
      case ps@NodeScan(pattern) => estimateNodeRow(pattern, ppc.runnerContext.graphModel)
      case pr@RelationshipScan(rel, left, right) => estimateRelationshipRow(rel, left, right, ppc.runnerContext.graphModel)
    }
  }

  def estimateTableSize(parent: Join, table1: PhysicalPlan, table2: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val estimateTable1 = estimate(table1, ppc)
    val estimateTable2 = estimate(table2, ppc)
    if (estimateTable1 <= estimateTable2) Join(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
    else Join(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table2, table1, ppc)
  }

  def joinRecursion(parent: Join, ppc: PhysicalPlannerContext, isSingleMatch: Boolean): PhysicalPlan = {
    val table1 = getBaseTable(parent.children.head, ppc, isSingleMatch)
    val table2 = getBaseTable(parent.children.last, ppc, isSingleMatch)

    if ((table1.isInstanceOf[NodeScan] || table1.isInstanceOf[RelationshipScan])
      && (table2.isInstanceOf[NodeScan] || table2.isInstanceOf[RelationshipScan])) {
      estimateTableSize(parent, table1, table2, ppc)
    } else Join(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
  }

  private def getBaseTable(plan: PhysicalPlan, ppc: PhysicalPlannerContext, isSingleMatch: Boolean): PhysicalPlan = plan match {
    case pj@Join(_, _, _) => joinRecursion(pj, ppc, isSingleMatch)
    case pm@Merge(mergeSchema, mergeOps, onMatch, onCreate) => {
      val res = joinRecursion(pm.children.head.asInstanceOf[Join], ppc, isSingleMatch)
      pm.withChildren(Seq(res))
    }
    case other => other
  }
}
