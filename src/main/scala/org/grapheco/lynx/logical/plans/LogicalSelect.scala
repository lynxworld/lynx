package org.grapheco.lynx.logical.plans

case class LogicalSelect(columns: Seq[(String, Option[String])])(val in: LogicalPlan) extends LogicalPlan(Some(in), None)
