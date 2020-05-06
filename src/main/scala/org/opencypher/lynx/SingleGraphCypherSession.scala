package org.opencypher.lynx

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.okapi.logical.impl._

case class LynxNode(id: Long, labels: Set[String], properties: CypherMap) extends Node[Long] {
  override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode =
    LynxNode(id: Long, labels: Set[String], properties)

  override type I = LynxNode
}

case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, properties: CypherMap) extends Relationship[Long] {
  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): LynxRelationship.this.type = ???

  override type I = this.type
}

case class LynxCypherRecords(meta: Map[String, CypherType], records: Iterator[CypherMap]) extends CypherRecords {
  override def collect: Array[CypherMap] = records.toArray

  override def iterator: Iterator[CypherMap] = records

  override def rows: Iterator[(String) => CypherValue] = records.map(x => (key: String) => CypherValue(x(key)))

  override def size: Long = records.size

  override def columnType: Map[String, CypherType] = meta

  override def physicalColumns: Seq[String] = meta.keys.toSeq

  override def show(implicit options: PrintOptions): Unit = {}

  def select(names: Array[String]) = LynxCypherRecords(meta.filterKeys(names.contains(_)),
    records.map(x => CypherMap(x.unwrap.filterKeys(names.contains(_)).toSeq: _*)))

  def eval(expr: Expr)(ctx: CypherMap, parameters: CypherMap): CypherValue = {
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

  def filter(expr: Expr, parameters: CypherMap) = LynxCypherRecords(meta,
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

  def project(expr: Expr): LynxCypherRecords = {
    expr match {
      case ElementProperty(NodeVar(name: String), key: PropertyKey) =>
        val qname = expr.withoutType
        LynxCypherRecords(meta ++ Map(qname -> expr.cypherType),
          records.map(x => x ++ CypherMap(qname -> x(name).asInstanceOf[LynxNode].properties(key.name))))
    }

  }
}

object Objects {
  val emptyResult: CypherResult = new CypherResult {
    override def getRecords: Option[Records] = ???

    override def plans: CypherQueryPlans = ???

    override def getGraph: Option[Graph] = None

    override def show(implicit options: PrintOptions): Unit = {}
  }
}

class SingleGraphCypherSession(graph: PropertyGraph, executor: QueryPlanExecutor) extends CypherSession with Logging {
  private val _catalog = new CypherCatalog()

  override def catalog: PropertyGraphCatalog = _catalog

  private val _catalogGraph = IRCatalogGraph("_graph", graph.schema)
  _catalog.store(_catalogGraph.qualifiedGraphName, graph)

  override type Result = CypherResult
  protected val logicalPlanner: LogicalPlanner = new LogicalPlanner(new LogicalOperatorProducer)

  protected val logicalOptimizer: LogicalOptimizer.type = LogicalOptimizer

  override def cypher(
                       query: String,
                       queryParameters: CypherMap = CypherMap.empty,
                       drivingTable: Option[CypherRecords] = None,
                       queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty): Result =
    cypherOnGraph(this.graph, query, queryParameters, drivingTable, queryCatalog)

  protected def planCypherQuery(
                                 graph: PropertyGraph,
                                 cypherQuery: CypherQuery,
                                 allParameters: CypherMap,
                                 inputFields: Set[Var],
                                 queryLocalCatalog: QueryLocalCatalog
                               ): Result = {
    val logicalPlan = planLogical(cypherQuery, graph, inputFields, queryLocalCatalog)
    planRelational(allParameters, logicalPlan, queryLocalCatalog)
  }

  protected def planLogical(
                             ir: CypherQuery,
                             graph: PropertyGraph,
                             inputFields: Set[Var],
                             queryLocalCatalog: QueryLocalCatalog
                           ): LogicalOperator = {
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources, queryLocalCatalog)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    optimizedLogicalPlan
  }

  protected def planRelational(
                                parameters: CypherMap,
                                logicalPlan: LogicalOperator,
                                queryLocalCatalog: QueryLocalCatalog
                              ): Result = {
    //val optimizedRelationalPlan = AnyCypherOptimizer.process(relationalPlan)
    executor.execute(parameters, logicalPlan, queryLocalCatalog)
  }

  override private[opencypher] def cypherOnGraph(
                                                  graph: PropertyGraph,
                                                  query: String,
                                                  parameters: CypherMap,
                                                  drivingTable: Option[CypherRecords],
                                                  queryCatalog: Map[QualifiedGraphName, PropertyGraph]):
  CypherResult = {
    val inputFields: Set[Var] = Set.empty
    val (stmt, extractedLiterals, semState) = CypherParser.process(query)(CypherParser.defaultContext)

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
      queryCatalog
    )

    val irOut = IRBuilder.process(stmt)(irBuilderContext)

    val ir = IRBuilder.extract(irOut)
    val queryLocalCatalog = IRBuilder.getContext(irOut).queryLocalCatalog

    def processIR(ir: CypherStatement): Result = ir match {
      case cq: CypherQuery =>
        if (PrintIr.isSet) {
          println("IR:")
          println(cq.pretty)
        }

        planCypherQuery(graph, cq, allParameters, inputFields, queryLocalCatalog)

      case CreateGraphStatement(targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, queryLocalCatalog)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph.asInstanceOf[PropertyGraph])
        Objects.emptyResult.asInstanceOf[Result]

      case CreateViewStatement(qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        Objects.emptyResult.asInstanceOf[Result]

      case DeleteGraphStatement(qgn) =>
        catalog.dropGraph(qgn)
        Objects.emptyResult.asInstanceOf[Result]

      case DeleteViewStatement(qgn) =>
        catalog.dropView(qgn)
        Objects.emptyResult.asInstanceOf[Result]
    }

    processIR(ir)
  }
}

trait QueryPlanExecutor {
  def execute(parameters: CypherMap, logicalPlan: LogicalOperator, queryLocalCatalog: QueryLocalCatalog): CypherResult
}