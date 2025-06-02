package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.physical.plans.{Aggregation, AllNode, AllRelationships, ApplyJoin, Distinct, Expand, Expand2, Filter, Join, Limit, MultiStepExpand, NodeScanByLabel2, NodeSeekByID, NodeSeekByIndex, OrderBy, PhysicalPlan, Project, RelationshipsByType, Skip, Union}

object Factors {
  val _factor: Map[Class[_ <: PhysicalPlan], Double] = Map(
    // 节点访问
    classOf[AllNode] -> 1.0,
    classOf[NodeScanByLabel2] -> 0.4,
    classOf[NodeSeekByID] -> 0.1,
    classOf[NodeSeekByIndex] -> 0.2,

    // 关系访问
    classOf[AllRelationships] -> 1.2,
    classOf[RelationshipsByType] -> 0.4,
    //    classOf[RelationshipScanByType] -> 0.5,
    //    classOf[RelationshipSeekByID] -> 0.1,

    // 扩展操作
    classOf[Expand] -> 0.1,
    classOf[Expand2] -> 0.1,
    classOf[MultiStepExpand] -> 0.6,

    // 过滤操作
    classOf[Filter] -> 0.2,

    // 连接操作
    classOf[Join] -> 0.8,
    classOf[ApplyJoin] -> 1.0,

    // 其他操作
    classOf[Project] -> 0.1,
    classOf[Aggregation] -> 0.7,
    classOf[OrderBy] -> 0.5,
    classOf[Limit] -> 0.1,
    classOf[Skip] -> 0.1,
    classOf[Distinct] -> 0.4,
    classOf[Union] -> 0.3
  )

  def apply(plan: PhysicalPlan): Double = _factor.getOrElse(plan.getClass, 1.0)
}
