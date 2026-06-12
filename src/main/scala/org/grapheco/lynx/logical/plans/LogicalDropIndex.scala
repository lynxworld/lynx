package org.grapheco.lynx.logical.plans

case class LogicalDropIndex(labelName: String, properties: List[String]) extends LeafLogicalPlan
