package org.opencypher.lynx

import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.lynx.planning.LynxPhysicalPlanner.PhysicalOperatorOps
import org.opencypher.lynx.planning.{GraphUnionAll, PhysicalOperator, ReturnGraph}
import org.opencypher.okapi.api.graph.{CypherQueryPlans, _}
import org.opencypher.okapi.api.table.{CypherRecords, CypherTable}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.logical.impl.LogicalOperator

object LynxRecords {
  def empty(header: RecordHeader = RecordHeader.empty)(implicit session: LynxSession): LynxRecords =
    new LynxRecords(header, LynxTable.empty(header.exprToColumn.map(x => x._2 -> x._1.cypherType).toSeq))

  def apply(header: RecordHeader, table: LynxTable, maybeDisplayNames: Option[Seq[String]] = None): LynxRecords =
    new LynxRecords(header, table, maybeDisplayNames)
}

//RecordHeader.exprToColumn={SimpleVar('n.name')->'n_name'}
//maybeDisplayNames=['n.name'], order will differ with RecordHeader
//table.schema={'n_name'->CTString}
//table.records=[{'n_name'->'bluejoe'}]
class LynxRecords(val header: RecordHeader, val table: LynxTable, maybeDisplayNames: Option[Seq[String]] = None) extends CypherRecords {
  lazy val mappingLogical2PhysicalColumns: Map[String, String] = {
    if (maybeDisplayNames.isDefined)
      header.exprToColumn.map(kv => kv._1.withoutType -> kv._2).filter(x => maybeDisplayNames.get.contains(x._1))
    else
      physicalColumns.map(key => key -> key).toMap
  }

  override def iterator: Iterator[CypherMap] = {
    table.rows.map { row =>
      new CypherMap(mappingLogical2PhysicalColumns.map(kv => kv._1 -> row(kv._2)))
    }
  }

  override def collect: Array[CypherMap] = iterator.toArray

  override def logicalColumns: Option[Seq[String]] = maybeDisplayNames

  override def physicalColumns: Seq[String] = header.columns.toSeq

  override def columnType: Map[String, CypherType] = header.exprToColumn.map(t => t._2 -> t._1.cypherType)

  override def rows: Iterator[String => CypherValue.CypherValue] =
    iterator.map(cm => (key) => cm(key))

  override def size: Long = table.size

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)
}

trait LynxResult extends CypherResult {
  override type Records = LynxRecords
  override type Graph = LynxPropertyGraph
}

object LynxResult {
  def apply(physicalPlan: PhysicalOperator, logicalPlan: LogicalOperator) = new LynxResultImpl(physicalPlan, Some(logicalPlan))

  class LynxResultImpl(physicalPlan: PhysicalOperator, maybeLogicalPlan: Option[LogicalOperator]) extends LynxResult {
    override type Records = LynxRecords
    override type Graph = LynxPropertyGraph

    override def getGraph: Option[Graph] = physicalPlan match {
      case r: ReturnGraph => Some(r.graph)
      case g: GraphUnionAll => Some(g.graph)
      case _ => None
    }

    override def getRecords: Option[Records] = {
      val alignedResult = physicalPlan.alignColumnsWithReturnItems
      val header = alignedResult.recordHeader
      val maybeDisplayNames = alignedResult.maybeReturnItems.map(_.map(_.name))
      val displayNames = maybeDisplayNames match {
        case s@Some(_) => s
        case None => Some(header.vars.map(_.withoutType).toSeq)
      }

      Some(LynxRecords(alignedResult.recordHeader, alignedResult.table, maybeDisplayNames))
    }

    override def show(implicit options: PrintOptions): Unit = records.show(options)

    override def plans: CypherQueryPlans = new CypherQueryPlans() {
      override def logical: String = maybeLogicalPlan.map(_.pretty).getOrElse(null)

      override def relational: String = physicalPlan.pretty
    }
  }

  def empty()(implicit session: LynxSession) = new LynxResult() {
    override type Records = LynxRecords
    override type Graph = LynxPropertyGraph

    override def getGraph: Option[Graph] = Some(LynxPropertyGraph.empty())

    override def getRecords: Option[Records] = Some(LynxRecords.empty())

    override def plans: CypherQueryPlans = new CypherQueryPlans() {
      override def logical: String = null

      override def relational: String = null
    }

    override def show(implicit options: PrintOptions): Unit = LynxRecords.empty().show
  }
}