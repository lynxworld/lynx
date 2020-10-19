package org.opencypher.lynx

import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.graph.{EmptyGraph, LynxPropertyGraph}
import org.opencypher.lynx.planning.{LynxPhysicalOptimizer, LynxPhysicalPlanner}
import org.opencypher.okapi.api.graph.{PropertyGraph, _}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.okapi.logical.impl.{LogicalOperator, _}

object LynxSession {
  val EMPTY_GRAPH_NAME = QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName("emptyGraph"))
}

class LynxSession extends CypherSession with Logging {
  override type Result = LynxResult
  protected val parser: CypherParser = CypherParser

  private implicit val session: LynxSession = this

  private[opencypher] def graphAt(qgn: QualifiedGraphName): Option[LynxPropertyGraph] =
    if (catalog.graphNames.contains(qgn)) Some(catalog.graph(qgn).asInstanceOf[LynxPropertyGraph]) else None

  private[opencypher] def createPlannerContext(parameters: CypherMap = CypherMap.empty, maybeDrivingTable: Option[LynxRecords] = None): LynxPlannerContext =
    LynxPlannerContext(graphAt, maybeDrivingTable, parameters)(session)

  override def cypher(
                       query: String,
                       queryParameters: CypherValue.CypherMap,
                       drivingTable: Option[CypherRecords],
                       queryCatalog: Map[QualifiedGraphName, PropertyGraph]
                     ): Result =
    cypherOnGraph(EmptyGraph(), query, queryParameters, drivingTable, queryCatalog)

  private def planCypherQuery(
                               graph: PropertyGraph,
                               cypherQuery: CypherQuery,
                               allParameters: CypherMap,
                               inputFields: Set[Var],
                               maybeDrivingTable: Option[LynxRecords],
                               queryLocalCatalog: QueryLocalCatalog
                             ): Result = {
    val optimizedLogicalPlan: LogicalOperator = planLogical(graph, cypherQuery, inputFields, queryLocalCatalog)
    val (ctx, physicalPlan) = planPhysical(maybeDrivingTable, allParameters, optimizedLogicalPlan, queryLocalCatalog)
    createCypherResult(physicalPlan, optimizedLogicalPlan, ctx)
  }

  private def planPhysical(
                            maybeDrivingTable: Option[LynxRecords],
                            parameters: CypherMap,
                            logicalPlan: LogicalOperator,
                            queryLocalCatalog: QueryLocalCatalog
                          ): (LynxPlannerContext, PhysicalOperator) = {
    //physical planning
    val ctx = createPlannerContext(parameters, maybeDrivingTable)

    val physicalPlan = createPhysicalPlan(logicalPlan, ctx)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val optimizedPlan = optimizePhysicalPlan(physicalPlan, ctx)
    logger.debug(s"Optimized physical plan: \r\n${optimizedPlan.pretty}")

    ctx -> optimizedPlan
  }

  private def planLogical(graph: PropertyGraph, cypherQuery: CypherQuery, inputFields: Set[Var], queryLocalCatalog: QueryLocalCatalog) = {
    //Logical planning
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources, queryLocalCatalog)
    val logicalPlan = createLogicalPlan(cypherQuery, logicalPlannerContext)

    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    //Logical optimization
    val optimizedLogicalPlan = optimizeLogicalPlan(logicalPlan, logicalPlannerContext)
    logger.debug(s"Optimized logical plan: \r\n${optimizedLogicalPlan.pretty}")

    optimizedLogicalPlan
  }

  protected val createLogicalPlan: (CypherQuery, LogicalPlannerContext) => LogicalOperator =
    (ir: CypherQuery, context: LogicalPlannerContext) => new LogicalPlanner(new LogicalOperatorProducer).process(ir)(context)

  protected val optimizeLogicalPlan: (LogicalOperator, LogicalPlannerContext) => LogicalOperator =
    (input: LogicalOperator, context: LogicalPlannerContext) => LogicalOptimizer.process(input)(context)

  protected val createPhysicalPlan: (LogicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: LogicalOperator, context: LynxPlannerContext) => LynxPhysicalPlanner.process(input)(context)

  protected val optimizePhysicalPlan: (PhysicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: PhysicalOperator, context: LynxPlannerContext) => LynxPhysicalOptimizer.process(input)(context)

  protected val createCypherResult: (PhysicalOperator, LogicalOperator, LynxPlannerContext) => LynxResult =
    (input: PhysicalOperator, logical: LogicalOperator, context: LynxPlannerContext) => LynxResult(input, logical)

  /**
   * Interface through which the user may (de-)register property graph datasources as well as read, write and delete property graphs.
   *
   * @return session catalog
   */
  override lazy val catalog: PropertyGraphCatalog = CypherCatalog()

  override private[opencypher] def cypherOnGraph(
                                                  propertyGraph: PropertyGraph,
                                                  query: String,
                                                  queryParameters: CypherMap,
                                                  drivingTable: Option[CypherRecords],
                                                  queryCatalog: Map[QualifiedGraphName, PropertyGraph]): Result = {

    val maybeRelationalRecords: Option[LynxRecords] = drivingTable.map(_.asInstanceOf[LynxRecords])

    val inputFields: Set[Var] = maybeRelationalRecords match {
      case Some(inputRecords) => inputRecords.header.vars
      case None => Set.empty[Var]
    }

    //AST construction
    val (stmt, extractedLiterals, semState) = parser.process(query, inputFields)(CypherParser.defaultContext)

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    val ambientGraphNew = mountAmbientGraph(propertyGraph)

    //IR translation
    val irBuilderContext = IRBuilderContext.initial(
      query,
      allParameters,
      semState,
      ambientGraphNew,
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
        logger.debug(s"IR: \r\n${cq.pretty}")
        planCypherQuery(propertyGraph, cq, allParameters, inputFields, maybeRelationalRecords, queryLocalCatalog)

      case CreateGraphStatement(targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(propertyGraph, innerQueryIr, allParameters, inputFields, maybeRelationalRecords, queryLocalCatalog)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph)
        LynxResult.empty

      case CreateViewStatement(qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        LynxResult.empty

      case DeleteGraphStatement(qgn) =>
        catalog.dropGraph(qgn)
        LynxResult.empty

      case DeleteViewStatement(qgn) =>
        catalog.dropView(qgn)
        LynxResult.empty
    }

    processIR(ir)
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val qgn = qgnGenerator.generate
    catalog.store(qgn, ambient)
    IRCatalogGraph(qgn, ambient.schema)
  }
}

case class LynxPlannerContext(sessionCatalog: QualifiedGraphName => Option[LynxPropertyGraph],
                              maybeInputRecords: Option[LynxRecords] = None,
                              parameters: CypherMap = CypherMap.empty,
                              var queryLocalCatalog: Map[QualifiedGraphName, LynxPropertyGraph] = Map.empty[QualifiedGraphName, LynxPropertyGraph]
                             )(implicit val session: LynxSession) {

  def resolveGraph(qgn: QualifiedGraphName): LynxPropertyGraph = queryLocalCatalog.get(qgn) match {
    case None => sessionCatalog(qgn).getOrElse(throw IllegalArgumentException(s"a graph at $qgn"))
    case Some(g) => g
  }

  override def toString: String = this.getClass.getSimpleName
}