package org.opencypher.lynx

import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.graph.{EmptyGraph, LynxPropertyGraph, ScanGraph, WritableScanGraph}
import org.opencypher.lynx.ir.{LynxIRBuilder, IRNode, PropertyGraphWriter, WritablePropertyGraph}
import org.opencypher.lynx.plan.{LynxPhysicalOptimizer, LynxPhysicalPlanner, PhysicalOperator, SimpleTableOperator}
import org.opencypher.okapi.api.graph.{PropertyGraph, _}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.table.{CypherRecords, CypherTable}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, Relationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.ir.api.{CypherStatement, _}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.okapi.logical.impl.{LogicalOperator, _}
import org.opencypher.v9_0.ast
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState

import scala.collection.{Seq, mutable}

class LynxSession extends CypherSession with Logging {
  override type Result = LynxResult
  type Id = Long

  private implicit val session: LynxSession = this

  //////////<---overridable vals
  protected val _parser: CypherParser = CypherParser
  protected val _tableOperator: TableOperator = new SimpleTableOperator
  protected val _createLogicalPlan: (CypherQuery, LogicalPlannerContext) => LogicalOperator =
    (ir: CypherQuery, context: LogicalPlannerContext) => new LogicalPlanner(new LogicalOperatorProducer).process(ir)(context)

  protected val _optimizeLogicalPlan: (LogicalOperator, LogicalPlannerContext) => LogicalOperator =
    (input: LogicalOperator, context: LogicalPlannerContext) => LogicalOptimizer.process(input)(context)

  protected val _createPhysicalPlan: (LogicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: LogicalOperator, context: LynxPlannerContext) => LynxPhysicalPlanner.process(input)(context)

  protected val _optimizePhysicalPlan: (PhysicalOperator, LynxPlannerContext) => PhysicalOperator =
    (input: PhysicalOperator, context: LynxPlannerContext) => LynxPhysicalOptimizer.process(input)(context)

  protected val _createCypherResult: (PhysicalOperator, LogicalOperator, LynxPlannerContext) => LynxResult =
    (input: PhysicalOperator, logical: LogicalOperator, context: LynxPlannerContext) => LynxResult(input, logical)

  //AST Statement-->CypherStatement
  protected val _translateASTIntoIR: (
    String, ast.Statement, CypherMap, SemanticState, IRCatalogGraph, Set[Var], Map[QualifiedGraphName, PropertyGraph]) => (CypherStatement, QueryLocalCatalog) =
    _defaultTranslate

  private val _cachedLogicalPlans = mutable.Map[(PropertyGraph, CypherQuery, CypherMap), LogicalOperator]()

  protected def getOrUpdateLogicalPlan(graph: PropertyGraph, cypherQuery: CypherQuery, parameters: CypherMap, logicalPlan: () => LogicalOperator): LogicalOperator =
    _cachedLogicalPlans.getOrElseUpdate((graph, cypherQuery, parameters), logicalPlan())

  private val _cachedCypherStatements = mutable.Map[(PropertyGraph, String, CypherMap), (CypherStatement, QueryLocalCatalog, CypherMap, Set[Var], Option[LynxRecords])]()

  def getOrUpdateCypherStatement(propertyGraph: PropertyGraph, query: String, queryParameters: CypherMap,
                                 createCypherStatement: () => (CypherStatement, QueryLocalCatalog, CypherMap, Set[Var], Option[LynxRecords])):
  (CypherStatement, QueryLocalCatalog, CypherMap, Set[Var], Option[LynxRecords]) = {
    _cachedCypherStatements.getOrElseUpdate((propertyGraph, query, queryParameters), createCypherStatement())
  }

  //////////overridable vals--->

  private[opencypher] def graphAt(qgn: QualifiedGraphName): Option[LynxPropertyGraph] =
    if (catalog.graphNames.contains(qgn)) Some(catalog.graph(qgn).asInstanceOf[LynxPropertyGraph]) else None

  private[opencypher] def createPlannerContext(parameters: CypherMap = CypherMap.empty, maybeDrivingTable: Option[LynxRecords] = None): LynxPlannerContext =
    LynxPlannerContext(graphAt, maybeDrivingTable, parameters)(session)

  def createPropertyGraph(scan: PropertyGraphScanner[Id]): LynxPropertyGraph = new ScanGraph[Id](scan)(session)

  def createPropertyGraph(scan: PropertyGraphScanner[Id], writer: PropertyGraphWriter[Id]): WritablePropertyGraph[Id] = new WritableScanGraph[Id](scan, writer)(session)

  def tableOperator: TableOperator = _tableOperator

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
    def createLogicalPlan() = {
      planLogical(graph, cypherQuery, inputFields, queryLocalCatalog)
    }

    val optimizedLogicalPlan: LogicalOperator = {
      if (maybeDrivingTable.isEmpty && queryLocalCatalog.registeredGraphs.isEmpty) {
        getOrUpdateLogicalPlan(graph, cypherQuery, allParameters, createLogicalPlan);
      }
      else {
        createLogicalPlan()
      }
    }

    val (ctx, physicalPlan) = planPhysical(maybeDrivingTable, allParameters, optimizedLogicalPlan, queryLocalCatalog)
    _createCypherResult(physicalPlan, optimizedLogicalPlan, ctx)
  }

  private def planPhysical(
                            maybeDrivingTable: Option[LynxRecords],
                            parameters: CypherMap,
                            logicalPlan: LogicalOperator,
                            queryLocalCatalog: QueryLocalCatalog
                          ): (LynxPlannerContext, PhysicalOperator) = {
    //physical planning
    val ctx = createPlannerContext(parameters, maybeDrivingTable)

    val physicalPlan = _createPhysicalPlan(logicalPlan, ctx)
    //logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val optimizedPlan = _optimizePhysicalPlan(physicalPlan, ctx)
    logger.debug(s"Optimized physical plan: \r\n${optimizedPlan.pretty}")

    ctx -> optimizedPlan
  }

  private def planLogical(graph: PropertyGraph, cypherQuery: CypherQuery, inputFields: Set[Var], queryLocalCatalog: QueryLocalCatalog) = {
    //Logical planning
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources, queryLocalCatalog)
    val logicalPlan = _createLogicalPlan(cypherQuery, logicalPlannerContext)

    //logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")

    //Logical optimization
    val optimizedLogicalPlan = _optimizeLogicalPlan(logicalPlan, logicalPlannerContext)
    logger.debug(s"Optimized logical plan: \r\n${optimizedLogicalPlan.pretty}")

    optimizedLogicalPlan
  }

  /**
   * Interface through which the user may (de-)register property graph datasources as well as read, write and delete property graphs.
   *
   * @return session catalog
   */
  override lazy val catalog: PropertyGraphCatalog = CypherCatalog()

  private def _defaultTranslate(
                                 query: String,
                                 statement: ast.Statement,
                                 allParameters: CypherMap,
                                 semState: SemanticState,
                                 workingGraph: IRCatalogGraph,
                                 inputFields: Set[Var],
                                 queryCatalog: Map[QualifiedGraphName, PropertyGraph]
                               ): (CypherStatement, QueryLocalCatalog) = {
    val irBuilderContext = IRBuilderContext.initial(
      query,
      allParameters,
      semState,
      workingGraph,
      qgnGenerator,
      catalog.listSources,
      catalog.view,
      inputFields,
      queryCatalog
    )

    LynxIRBuilder.process(statement, irBuilderContext, this)
  }

  override private[opencypher] def cypherOnGraph(
                                                  propertyGraph: PropertyGraph,
                                                  query: String,
                                                  queryParameters: CypherMap,
                                                  drivingTable: Option[CypherRecords],
                                                  queryCatalog: Map[QualifiedGraphName, PropertyGraph]): Result = {

    def createStatement(): (CypherStatement, QueryLocalCatalog, CypherMap, Set[Var], Option[LynxRecords]) = {
      val maybeRelationalRecords: Option[LynxRecords] = drivingTable.map(_.asInstanceOf[LynxRecords])

      val inputFields: Set[Var] = maybeRelationalRecords match {
        case Some(inputRecords) => inputRecords.header.vars
        case None => Set.empty[Var]
      }

      //AST construction
      val (stmt: Statement, extractedLiterals: Map[String, Any], semState: SemanticState) =
        _parser.process(query, inputFields)(CypherParser.defaultContext)

      logger.debug(s"AST: ${stmt.asCanonicalStringVal}")

      val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
      val allParameters = queryParameters ++ extractedParameters

      val ambientGraphNew = mountAmbientGraph(propertyGraph)

      //AST Statement-->CypherStatement
      //IR translation
      val (cypherStatement, queryLocalCatalog) = _translateASTIntoIR(
        query,
        stmt,
        allParameters,
        semState,
        ambientGraphNew,
        inputFields,
        queryCatalog)

      (cypherStatement, queryLocalCatalog, allParameters, inputFields, maybeRelationalRecords)
    }

    val (ir, queryLocalCatalog, allParameters, inputFields, maybeRelationalRecords) = {
      if (drivingTable.isEmpty && queryCatalog.isEmpty)
        getOrUpdateCypherStatement(propertyGraph, query, queryParameters, createStatement)
      else
        createStatement()
    }

    def processIR(ir: CypherStatement): Result = ir match {
      case ces: CreateElementStatement[Id] =>
        propertyGraph.asInstanceOf[WritablePropertyGraph[Id]].createElements(ces.nodes, ces.rels)
        LynxResult.empty

      case cq: CypherQuery =>
        //logger.debug(s"IR: \r\n${cq.pretty}")
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

trait PropertyGraphScanner[Id] {
  def nodeAt(id: Id): Node[Id]

  def schema: PropertyGraphSchema = PropertyGraphSchema.empty

  def allNodes(): Iterable[Node[Id]]

  def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Id]] = {
    allNodes().filter { node =>
      if (exactLabelMatch)
        node.labels.equals(labels)
      else
        node.labels.intersect(labels).size == labels.size
    }
  }

  def allRelationships(): Iterable[Relationship[Id]]

  def allRelationships(relTypes: Set[String]): Iterable[Relationship[Id]] = {
    allRelationships().filter(rel => (relTypes.contains(rel.relType)))
  }
}