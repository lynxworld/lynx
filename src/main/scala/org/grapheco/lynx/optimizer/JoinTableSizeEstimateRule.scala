package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical._
import org.grapheco.lynx.runner.GraphModel
import org.opencypher.v9_0.expressions.{Literal, MapExpression, NodePattern, RelationshipPattern}

import scala.collection.mutable

/**
 * rule to estimate two tables' size in PPTJoin
 * mark the bigger table's position
 */
object JoinTableSizeEstimateRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode => {
        pnode.children match {
          case Seq(pj@PPTJoin(filterExpr, isSingleMatch, joinType)) => {
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

  def estimate(table: PPTNode, ppc: PhysicalPlannerContext): Long = {
    table match {
      case ps@PPTNodeScan(pattern) => estimateNodeRow(pattern, ppc.runnerContext.graphModel)
      case pr@PPTRelationshipScan(rel, left, right) => estimateRelationshipRow(rel, left, right, ppc.runnerContext.graphModel)
    }
  }

  def estimateTableSize(parent: PPTJoin, table1: PPTNode, table2: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    val estimateTable1 = estimate(table1, ppc)
    val estimateTable2 = estimate(table2, ppc)
    if (estimateTable1 <= estimateTable2) PPTJoin(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
    else PPTJoin(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
  }

  def joinRecursion(parent: PPTJoin, ppc: PhysicalPlannerContext, isSingleMatch: Boolean): PPTNode = {
    val t1 = parent.children.head
    val t2 = parent.children.last

    val table1 = t1 match {
      case pj@PPTJoin(filterExpr, isSingleMatch, joinType) => joinRecursion(pj, ppc, isSingleMatch)
      case pm@PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t1
    }
    val table2 = t2 match {
      case pj@PPTJoin(filterExpr, isSingleMatch, joinType) => joinRecursion(pj, ppc, isSingleMatch)
      case pm@PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t2
    }

    if ((table1.isInstanceOf[PPTNodeScan] || table1.isInstanceOf[PPTRelationshipScan])
      && (table2.isInstanceOf[PPTNodeScan] || table2.isInstanceOf[PPTRelationshipScan])) {
      estimateTableSize(parent, table1, table2, ppc)
    }
    else PPTJoin(parent.filterExpr, parent.isSingleMatch, parent.joinType)(table1, table2, ppc)
  }
}
