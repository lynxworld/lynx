package org.grapheco.lynx.logical.plans

import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName}

case class LogicalProcedureCall(procedureNamespace: Namespace,
                                procedureName: ProcedureName,
                                declaredArguments: Option[Seq[Expression]]) extends LogicalPlan(None, None)
