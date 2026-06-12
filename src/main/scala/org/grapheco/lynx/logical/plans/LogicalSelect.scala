package org.grapheco.lynx.logical.plans

case class LogicalSelect(columns: Seq[(String, Option[String])])(val in: LogicalPlan) extends SingleLogicalPlan(Some(in))
