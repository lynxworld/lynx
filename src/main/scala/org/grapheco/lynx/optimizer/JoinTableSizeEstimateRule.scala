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
      case pnode: PhysicalPlan => {
        pnode.children match {
          case Seq(pj@Join(filterExpr, isSingleMatch, joinType)) => {
            val res = joinRecursion(pj, ppc, isSingleMatch)
            pnode.withChildren(Seq(res))
          }
          case _ => pnode
        }
      }
    }
  )

  def estimateNodeRow(pattern: NodePattern, graphModel: GraphModel): Long = {
    val countMap = mutable.Map[String, Long]()
    val labels = pattern.labels.map(l => l.name)
    val prop = pattern.properties.map({
      case MapExpression(items) => {
        items.map(
          p => {
            p._2 match {
              case b: Literal => (p._1.name, b.value)
              case _ => (p._1.name, null)
            }
          }
        )
      }
      case _ => return 0
    })

    if (labels.nonEmpty) {
      val minLabelAndCount = labels.map(label => (label, graphModel._helper.estimateNodeLabel(label))).minBy(f => f._2)

      if (prop.isDefined) {
        prop.get.map(f => graphModel._helper.estimateNodeProperty(minLabelAndCount._1, f._1, f._2)).min
      }
      else minLabelAndCount._2
    }
    else graphModel.statistics.numNode
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
    val t1 = parent.children.head
    val t2 = parent.children.last

    val table1 = t1 match {
      case pj@Join(filterExpr, isSingleMatch, joinType) => joinRecursion(pj, ppc, isSingleMatch)
      case pm@Merge(mergeSchema, mergeOps, onMatch, onCreate) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[Join], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t1
    }
    val table2 = t2 match {
      case pj@Join(filterExpr, isSingleMatch, joinType) => joinRecursion(pj, ppc, isSingleMatch)
      case pm@Merge(mergeSchema, mergeOps, onMatch, onCreate) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[Join], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t2
    }

    if ((table1.isInstanceOf[NodeScan] || table1.isInstanceOf[RelationshipScan])
      && (table2.isInstanceOf[NodeScan] || table2.isInstanceOf[RelationshipScan])) {
      estimateTableSize(parent, table1, table2, ppc)
    }
    else Join(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
  }
}
