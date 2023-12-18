package org.grapheco.lynx.logical.plans

case class LogicalCreateIndex(labelName: String, properties: List[String]) extends LogicalPlan(None, None)
