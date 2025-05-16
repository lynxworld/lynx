package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.physical.planner.translators.MetaData._
import org.grapheco.lynx.physical.plans.{AllNodes, NodeScanByLabel, NodeSeekByID, NodeSeekByIndex, PhysicalPlan}
import org.grapheco.lynx.types.structural.LynxNodeLabel
import scala.collection.JavaConverters._

case class Candidate(var plan: PhysicalPlan, var cost:BigDecimal, var cardinal:Long)

object CostCalculator {
  val _factor: Map[Class[_<:PhysicalPlan], Double] = Map(
    // nodes
    classOf[AllNodes] -> 1,
    classOf[NodeScanByLabel] -> 0.4,
    classOf[NodeSeekByID] -> 0.1,
    classOf[NodeSeekByIndex] -> 0.2,
  )

  def cost(plan: PhysicalPlan): Candidate = plan match{
  // TODO
  case n:AllNodes => Candidate(n, _factor(classOf[AllNodes])*nodeNum, nodeNum)
  case n:NodeScanByLabel => Candidate(n, _factor(classOf[NodeScanByLabel])*nodeNum, labelNum(n.pattern.labels.head))
  case n:NodeSeekByID => Candidate(n, _factor(classOf[NodeSeekByID])*nodeNum,1)
  case n:NodeSeekByIndex => Candidate(n, _factor(classOf[NodeSeekByIndex])*nodeNum,
    labelNum(n.pattern.labels.head)/ propertiesTypeNum(n.pattern.labels.head)(n.pattern.properties.get.arguments.head.asInstanceOf[org.opencypher.v9_0.expressions.Property].propertyKey.name))
  //case _ => throw new UnsupportedOperationException("Unsupported plan type")
  }
}
