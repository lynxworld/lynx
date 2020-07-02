package org.opencypher.lynx

import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.FormatUtils
import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.okapi.logical.impl._

import scala.collection.mutable

object LynxCypherRecords {
  def nodes(colName: String, nodes: Stream[LynxNode]) = new LynxCypherRecords(Map(colName -> CTNode), nodes.map(node => CypherMap(colName -> node)))

  def rels(colName: String, rels: Stream[LynxRelationship]) = new LynxCypherRecords(Map(colName -> CTRelationship), rels.map(rel => CypherMap(colName -> rel)))
}

class LynxCypherRecords(meta: Map[String, CypherType], records: Stream[CypherMap]) extends CypherRecords {
  override def collect: Array[CypherMap] = records.toArray

  override def iterator: Iterator[CypherMap] = records.iterator

  override def rows: Iterator[(String) => CypherValue] = records.iterator.map(x => (key: String) => CypherValue(x(key)))

  override def size: Long = records.size

  override def columnType: Map[String, CypherType] = meta

  override def physicalColumns: Seq[String] = meta.keys.toSeq

  override def show(implicit options: PrintOptions): Unit = {
    val rowsShown =
      if (size > 20) {
        println("only showing 20 first rows...")
        records.take(20)
      }
      else
        records

    FormatUtils.printTable(physicalColumns, rowsShown.map(row => {
      physicalColumns.map(col => row.apply(col).toString())
    }).toArray.toSeq)
  }

  def select(names: Array[String]) = new LynxCypherRecords(meta.filterKeys(names.contains(_)),
    records.map(x => CypherMap(x.unwrap.filterKeys(names.contains(_)).toSeq: _*)))

  //FIXME: use Eval
  private def eval(expr: Expr)(ctx: CypherMap, parameters: CypherMap): CypherValue = {
    expr match {
      case NodeVar(name) =>
        ctx(name)
      case ElementProperty(propertyOwner: Expr, key: PropertyKey) =>
        eval(propertyOwner)(ctx, parameters) match {
          case node: LynxNode => node.properties(key.name)
          case map: CypherMap => map(key.name)
        }
      case Param(name: String) =>
        parameters(name)
    }
  }

  def filter(expr: Expr, parameters: CypherMap) = new LynxCypherRecords(meta,
    records.filter { map =>
      expr match {
        case GreaterThan(lhs: Expr, rhs: Expr) =>
          (eval(lhs)(map, parameters), eval(rhs)(map, parameters)) match {
            case (CypherBigDecimal(v1), CypherBigDecimal(v2)) => v1.compareTo(v2) > 0
            case (CypherInteger(v1), CypherInteger(v2)) => v1.compareTo(v2) > 0
            case (CypherFloat(v1), CypherFloat(v2)) => v1.compareTo(v2) > 0
          }
      }
    })

  def project(expr: Expr, parameters: CypherMap): LynxCypherRecords = {
    val qname = expr.withoutType
    new LynxCypherRecords(meta ++ Map(qname -> expr.cypherType),
      records.map(x => x ++ CypherMap(qname -> Eval.eval(expr, x, parameters))))
  }
}

object EmptyCypherResult extends CypherResult {
  override def getRecords: Option[Records] = None

  override def plans: CypherQueryPlans = new CypherQueryPlans() {
    override def logical: String = "logical"

    override def relational: String = "relational"
  }

  override def getGraph: Option[Graph] = None

  override def show(implicit options: PrintOptions): Unit = {}
}

class LynxCypherSession(graph: PropertyGraph, executor: QueryPlanner) extends CypherSession with Logging {
  private val _catalog = new CypherCatalog()
  private val _compiled = mutable.Map[String, (Set[Var], CypherMap, CypherStatement, QueryLocalCatalog)]()

  override def catalog: PropertyGraphCatalog = _catalog

  private val _catalogGraph = IRCatalogGraph("_graph", graph.schema)
  _catalog.store(_catalogGraph.qualifiedGraphName, graph)

  override type Result = CypherResult
  protected val logicalPlanner: LogicalPlanner = new LogicalPlanner(new LogicalOperatorProducer)

  override def cypher(
                       query: String,
                       queryParameters: CypherMap = CypherMap.empty,
                       drivingTable: Option[CypherRecords] = None,
                       queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty): Result =
    cypherOnGraph(this.graph, query, queryParameters, drivingTable, queryCatalog)

  private def planCypherQuery(
                               graph: PropertyGraph,
                               cypherQuery: CypherQuery,
                               allParameters: CypherMap,
                               inputFields: Set[Var],
                               queryLocalCatalog: QueryLocalCatalog
                             ): Result = {
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources, queryLocalCatalog)
    //logical planning
    val logicalPlan = logicalPlanner(cypherQuery)(logicalPlannerContext)
    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    //logical plan optimization
    val optimizedLogicalPlan = executor.logicalPlanOptimized(logicalPlan, logicalPlannerContext)
    logger.debug(s"optimized logical plan: \r\n${optimizedLogicalPlan.pretty}")

    //physical planning
    val physicalPlan = executor.physicalPlan(allParameters, optimizedLogicalPlan, queryLocalCatalog)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val ctx = LynxPhysicalPlanContext(allParameters)
    val res = physicalPlan.execute(ctx)

    new CypherResult {
      type Records = LynxCypherRecords

      type Graph = PropertyGraph

      override def getGraph: Option[Graph] = Some(graph)

      override def getRecords: Option[Records] = Some(res)

      override def plans: CypherQueryPlans = new CypherQueryPlans() {
        override def logical: String = optimizedLogicalPlan.pretty

        override def relational: String = physicalPlan.pretty
      }

      override def show(implicit options: PrintOptions): Unit = res.show(options)
    }
  }

  override private[opencypher] def cypherOnGraph(
                                                  graph: PropertyGraph,
                                                  query: String,
                                                  parameters: CypherMap,
                                                  drivingTable: Option[CypherRecords],
                                                  queryCatalog: Map[QualifiedGraphName, PropertyGraph]): CypherResult = {
    //compilation
    val (inputFields: Set[Var], allParameters: CypherMap, ir: CypherStatement, queryLocalCatalog: QueryLocalCatalog) =
      _compiled.getOrElseUpdate(query,
        compileCypherQuery(query, parameters, queryCatalog))

    ir match {
      case cq: CypherQuery =>
        logger.debug(s"ir query: \r\n${cq.pretty}")
        planCypherQuery(graph, cq, allParameters, inputFields, queryLocalCatalog)

      case CreateGraphStatement(targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, queryLocalCatalog)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph.asInstanceOf[PropertyGraph])
        EmptyCypherResult

      case CreateViewStatement(qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        EmptyCypherResult

      case DeleteGraphStatement(qgn) =>
        catalog.dropGraph(qgn)
        EmptyCypherResult

      case DeleteViewStatement(qgn) =>
        catalog.dropView(qgn)
        EmptyCypherResult
    }
  }

  private def compileCypherQuery(query: String, parameters: CypherMap, queryCatalog: Map[QualifiedGraphName, PropertyGraph]): (Set[Var], CypherMap, CypherStatement, QueryLocalCatalog) = {
    val inputFields: Set[Var] = Set.empty
    val (stmt, extractedLiterals, semState) = CypherParser.process(query)(CypherParser.defaultContext)

    logger.debug(s"parsed query: \r\n${stmt}")

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = parameters ++ extractedParameters
    implicit val session = this

    val irBuilderContext = IRBuilderContext.initial(
      query,
      allParameters,
      semState,
      _catalogGraph,
      qgnGenerator,
      catalog.listSources,
      catalog.view,
      inputFields,
      queryCatalog)

    val ir = IRBuilder.apply(stmt)(irBuilderContext)

    val queryLocalCatalog = irBuilderContext.queryLocalCatalog
    (inputFields, allParameters, ir, queryLocalCatalog)
  }
}