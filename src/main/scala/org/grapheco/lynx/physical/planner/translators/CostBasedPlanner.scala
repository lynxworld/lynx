package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.logical.plans.{GraphPattern, GraphPatternEdge, GraphPatternMatch, GraphPatternNode}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.planner.translators.MetaData._
import org.grapheco.lynx.physical.plans.{AllNodes, Filter, NodesPlanFactory, PhysicalPlan, PhysicalPlanBuffer, RelationshipsPlanFactory}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxType

import scala.collection.mutable

class CostBasedPlanner()(implicit ppc: PhysicalPlannerContext ){
  private val records: mutable.Map[Set[GraphPatternNode], Candidate] = mutable.HashMap.empty
  //动态规划
  def plan(graph: GraphPattern): PhysicalPlan = {
    //获取单个节点最优计划
    val nodes = graph.allNodes.toSet
    nodes.foreach { node =>
      records.put(Set(node), nodePlan(node))
    }
    records(nodes).plan
    //println(records)
//    if(nodes.size>1){
//      //获取最优连接顺序
//      for (i <- 2 to nodes.size) {
//        nodes.subsets(i).foreach { set =>
//          val joinPlan = mergePlans(set)
//          if (joinPlan.plan!=null) {
//            records.put(set,joinPlan)
//          }
//        }
//      }
//    }
//    print(records(nodes))
    //println(records)
  }

  //单个节点最优计划
  private def nodePlan(node: GraphPatternNode): Candidate = {
    val factory = NodesPlanFactory(node.variableName.get)
    val localCandidate: Set[PhysicalPlan] = Set.empty +
    // C1: allNodes
    PhysicalPlanBuffer(factory.allNodes()) .andThen(Filter(node.properties.get)).plan+
    //PhysicalPlanBuffer(factory.allNodes()).plan +
    // C2: nodeScanByLabel
    (if (node.labels.nonEmpty) {
      PhysicalPlanBuffer(factory.nodeScan(node)).plan
    } else {
      null
    })+
    // C3: nodeSeekByIdIndex
    (if (node.labels.nonEmpty&& node.properties.isDefined
      && node.properties.get.arguments.head.asInstanceOf[org.opencypher.v9_0.expressions.Property].propertyKey.name == "id"){
      PhysicalPlanBuffer(factory.seekByID(node.properties.get)).plan
    } else {
      null
    })+
    // C4: nodeSeekByIndex
    (if (node.labels.nonEmpty && node.properties.isDefined
      && indexSeq.contains((node.labels.head,node.properties.get.arguments.head.asInstanceOf[org.opencypher.v9_0.expressions.Property].propertyKey.name))) {
      PhysicalPlanBuffer(factory.seekByIndex(node)).plan
    } else {
      null
    })
    // ...
    localCandidate.filter(_!= null).map(CostCalculator.cost).minBy(_.cost)
  }

  private def triplePlan(start: GraphPatternNode, rel: GraphPatternEdge, end: GraphPatternNode): Candidate = {
    /*
    For a triple (a)-[b]-(c), has 3 types of candidate:
    1. Relationship(b)->Filter(a-b-c)
    2. Node(a)->Expand(a-b-c)
    3. Node(c)->Expand(a-b-c)
     */
    val factory = RelationshipsPlanFactory(rel.variableName.get)
    val localCandidate: Set[Candidate] = Set.empty
    // ... TODO
    localCandidate.minBy(_.cost)
  }

  def mergePlans(nodeSet: Set[GraphPatternNode]): Candidate= {
    /*
    For a complex graph (a)-b-(c)-d-(e)-f-(g), has some types of candidate:
    1. ExpandOne: {(a)-b-(c)-d-(e)}->Expand(e->g)
    2. JOIN: {(a)-b-(c)}, {(e)-f-(g)} by (c)-d-(e)
    3. ... maybe other
     */
    val localCandidate: Set[Candidate] = Set.empty
    // ... TODO
    localCandidate.minBy(_.cost)
  }
}
