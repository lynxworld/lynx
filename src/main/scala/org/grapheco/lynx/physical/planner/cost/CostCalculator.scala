package org.grapheco.lynx.physical.planner.cost

import org.grapheco.lynx.LynxException
import org.grapheco.lynx.physical.planner.translators.MetaData._
import org.grapheco.lynx.physical.plans._
import org.grapheco.lynx.types.structural.LynxRelationshipType

trait CostCalculator {
  def estimate(plan: Candidate): Candidate
}

class DefaultCostCalculator(estimator: CardinalityEstimator) extends CostCalculator {
  // 各类物理计划的成本因子
  private def factor(p: PhysicalPlan): Double = Factors.apply(p)

  def estimate(candidate: Candidate): Candidate = {
    val cost = planCost(candidate.plan)
    candidate.copy(cost = cost._1, cardinal = cost._2)
  }

  private def planCost(plan: PhysicalPlan): (Double, Long) = plan match {
    // 节点访问计划
//    case n: AllNode =>
//      Candidate(n, factor(n) * nodeNum, nodeNum)
//
//    case n: NodeScanByLabel2 =>
//      val labelCardinality = labelNum(n.labelName)
//      Candidate(n, factor(n) * nodeNum, labelCardinality)
//
//    case n: NodeSeekByID =>
//      Candidate(n, factor(n) * nodeNum, 1)
    case lp: LeafPhysicalPlan => {
      val car = estimator.estimate(lp)(Seq.empty)
      println(s"${lp.getClass.getSimpleName} cost: ${car * factor(lp)} cardinal: ${car}")
      (car * factor(lp), car)
    }

    case sp: SinglePhysicalPlan => {
      val (child_cost, child_car) = planCost(sp.in)
      val car = estimator.estimate(sp)(Seq(child_car))
      println(s"${sp.getClass.getSimpleName} cost: ${car * factor(sp) + child_cost} cardinal: ${car}")
      (car * factor(sp) + child_cost, car)
    }

    case mp: PhysicalPlan => {
      val children = mp.children.map(planCost)
      val car = estimator.estimate(mp)(children.map(_._2))
      (car * factor(mp) + children.map(_._1).sum, car)
    }

    case _ => throw LynxException("Unsupported plan type")

//    case n: NodeSeekByIndex =>
//      val label = n.pattern.labels.head
//      val propName = n.pattern.expressions.get.arguments.head.asInstanceOf[org.opencypher.v9_0.expressions.Property].propertyKey.name
//      val selectivity = 1.0 / propertiesTypeNum(label)(propName)
//      val cardinality = Math.max(1, (labelNum(label) * selectivity).toLong)
//      Candidate(n, factor(n) * nodeNum, cardinality)

    // 关系访问计划
//    case r: AllRelationships =>
//      Candidate(r, _factor(classOf[AllRelationships]) * relNum, relNum)

//    case r: RelationshipScanByType =>
//      val typeCardinality = typeNum(r.pattern.types.head)
//      Candidate(r, _factor(classOf[RelationshipScanByType]) * relNum, typeCardinality)
//
//    case r: RelationshipSeekByID =>
//      Candidate(r, _factor(classOf[RelationshipSeekByID]) * relNum, 1)

    // 扩展操作
//    case e: Expand =>
//      val inputCandidate = cost(e.left.get)
//      val avgDegree = if (e.pattern.direction == org.grapheco.lynx.logical.plans.BOTH) avgDegree * 2 else avgDegree
//      val expandFactor = if (e.pattern.types.nonEmpty) typeSelectivity(e.pattern.types.head) else 1.0
//      val outputCardinality = (inputCandidate.cardinal * avgDegree * expandFactor).toLong
//      val expandCost = _factor(classOf[Expand]) * outputCardinality
//      Candidate(e, inputCandidate.cost + expandCost, outputCardinality)
//
//    case e: MultiStepExpand =>
//      val inputCandidate = cost(e.left.get)
//      val steps = e.pattern.length._2 // 最大步数
//      val avgDegree = if (e.pattern.direction == org.grapheco.lynx.logical.plans.BOTH) avgDegree * 2 else avgDegree
//      val expandFactor = if (e.pattern.types.nonEmpty) typeSelectivity(e.pattern.types.head) else 1.0
//      // 使用几何级数估计多步扩展的基数
//      val outputCardinality = (inputCandidate.cardinal * Math.pow(avgDegree * expandFactor, steps)).toLong
//      val expandCost = _factor(classOf[MultiStepExpand]) * outputCardinality
//      Candidate(e, inputCandidate.cost + expandCost, outputCardinality)

    // 过滤操作
//    case f: Filter =>
//      val inputCandidate = cost(f.left.get)
//      // 假设过滤器的选择率为0.3
//      val filterSelectivity = 0.3
//      val outputCardinality = (inputCandidate.cardinal * filterSelectivity).toLong
//      val filterCost = factor(f) * inputCandidate.cardinal
//      Candidate(f, inputCandidate.cost + filterCost, outputCardinality)
//
//    // 连接操作
//    case j: Join =>
//      val leftCandidate = cost(j.left.get)
//      val rightCandidate = cost(j.right.get)
//      // 估计连接后的基数，假设为两表大小的乘积乘以连接选择率
//      val joinSelectivity = 0.1
//      val outputCardinality = (leftCandidate.cardinal * rightCandidate.cardinal * joinSelectivity).toLong
//      val joinCost = factor(j) * (leftCandidate.cardinal + rightCandidate.cardinal)
//      Candidate(j, leftCandidate.cost + rightCandidate.cost + joinCost, outputCardinality)

//    case j: ApplyJoin =>
//      val leftCandidate = cost(j.left.get)
//      val rightCandidate = cost(j.right.get)
//      // Apply Join通常每个左侧记录都会执行右侧计划
//      val outputCardinality = leftCandidate.cardinal * rightCandidate.cardinal
//      val joinCost = _factor(classOf[ApplyJoin]) * leftCandidate.cardinal * rightCandidate.cost.toDouble
//      Candidate(j, leftCandidate.cost + joinCost, outputCardinality)

    // 投影操作
//    case p: Project =>
//      val inputCandidate = cost(p.left.get)
//      // 投影不改变基数
//      val projectCost = _factor(classOf[Project]) * inputCandidate.cardinal
//      Candidate(p, inputCandidate.cost + projectCost, inputCandidate.cardinal)
//
//    // 聚合操作
//    case a: Aggregation =>
//      val inputCandidate = cost(a.left.get)
//      // 估计聚合后的基数，假设为输入基数的0.1
//      val aggregationFactor = 0.1
//      val outputCardinality = Math.max(1, (inputCandidate.cardinal * aggregationFactor).toLong)
//      val aggregationCost = _factor(classOf[Aggregation]) * inputCandidate.cardinal
//      Candidate(a, inputCandidate.cost + aggregationCost, outputCardinality)
//
//    // 排序操作
//    case o: OrderBy =>
//      val inputCandidate = cost(o.left.get)
//      // 排序不改变基数，但成本与nlogn相关
//      val sortCost = _factor(classOf[OrderBy]) * inputCandidate.cardinal * Math.log(inputCandidate.cardinal + 1)
//      Candidate(o, inputCandidate.cost + sortCost, inputCandidate.cardinal)
//
//    // 限制操作
//    case l: Limit =>
//      val inputCandidate = cost(l.left.get)
//      // 限制后的基数为limit值或输入基数中的较小值
//      val limitValue = extractLimitValue(l)
//      val outputCardinality = Math.min(limitValue, inputCandidate.cardinal)
//      val limitCost = _factor(classOf[Limit]) * inputCandidate.cardinal
//      Candidate(l, inputCandidate.cost + limitCost, outputCardinality)
//
//    // 跳过操作
//    case s: Skip =>
//      val inputCandidate = cost(s.left.get)
//      // 跳过后的基数为输入基数减去skip值，不小于0
//      val skipValue = extractSkipValue(s)
//      val outputCardinality = Math.max(0, inputCandidate.cardinal - skipValue)
//      val skipCost = _factor(classOf[Skip]) * inputCandidate.cardinal
//      Candidate(s, inputCandidate.cost + skipCost, outputCardinality)
//
//    // 去重操作
//    case d: Distinct =>
//      val inputCandidate = cost(d.left.get)
//      // 估计去重后的基数，假设为输入基数的0.7
//      val distinctFactor = 0.7
//      val outputCardinality = (inputCandidate.cardinal * distinctFactor).toLong
//      val distinctCost = _factor(classOf[Distinct]) * inputCandidate.cardinal
//      Candidate(d, inputCandidate.cost + distinctCost, outputCardinality)
//
//    // 合并操作
//    case u: Union =>
//      val leftCandidate = cost(u.left.get)
//      val rightCandidate = cost(u.right.get)
//      // 合并后的基数为两个输入的基数之和
//      val outputCardinality = leftCandidate.cardinal + rightCandidate.cardinal
//      val unionCost = _factor(classOf[Union]) * (leftCandidate.cardinal + rightCandidate.cardinal)
//      Candidate(u, leftCandidate.cost + rightCandidate.cost + unionCost, outputCardinality)

    // 其他未处理的计划类型
//    case _ =>
//      // 对于未明确处理的计划类型，递归计算子计划成本并汇总
//      val childrenCandidates = plan.children.map(cost)
//      val childrenCost = childrenCandidates.map(_.cost).sum
//      val childrenCardinality = if (childrenCandidates.isEmpty) 1L else childrenCandidates.map(_.cardinal).product
//      // 使用默认因子0.5
//      val defaultFactor = 0.5
//      val planCost = defaultFactor * childrenCardinality
//      Candidate(plan, childrenCost + planCost, childrenCardinality)
  }

  /**
   * 提取Limit操作的限制值
   */
  private def extractLimitValue(limit: Limit): Long = {
    // 根据实际的Limit类实现来提取limit值
    // 这里假设有一个方法可以获取limit值
    try {
      val limitExpr = limit.getClass.getMethod("expr").invoke(limit)
      val value = limitExpr.toString.toLong
      Math.max(1, value)
    } catch {
      case _: Exception => 10 // 默认值
    }
  }

  /**
   * 提取Skip操作的跳过值
   */
  private def extractSkipValue(skip: Skip): Long = {
    // 根据实际的Skip类实现来提取skip值
    // 这里假设有一个方法可以获取skip值
    try {
      val skipExpr = skip.getClass.getMethod("expr").invoke(skip)
      val value = skipExpr.toString.toLong
      Math.max(0, value)
    } catch {
      case _: Exception => 0 // 默认值
    }
  }

  /**
   * 计算关系类型的选择率
   */
//  private def typeSelectivity(relType: LynxRelationshipType): Double = {
//    val specificTypeCount = typeNum(relType)
//    if (relNum > 0) specificTypeCount.toDouble / relNum else 1.0
//  }
}
