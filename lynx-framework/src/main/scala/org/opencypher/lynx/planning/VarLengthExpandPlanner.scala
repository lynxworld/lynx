package org.opencypher.lynx.planning

import org.opencypher.lynx.planning.LynxPhysicalPlanner.{process, _}
import org.opencypher.lynx.{LynxPlannerContext, RecordHeader, RecordHeaderException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.LogicalOperator

sealed trait ExpandDirection

case object Outbound extends ExpandDirection

case object Inbound extends ExpandDirection

abstract class VarLengthExpandPlanner {

  def source: Var

  def list: Var

  def edgeScan: Var

  def target: Var

  def lower: Int

  def upper: Int

  def sourceOp: LogicalOperator

  def relEdgeScanOp: PhysicalOperator

  def targetOp: LogicalOperator

  def isExpandInto: Boolean

  def plan: PhysicalOperator

  implicit val context: LynxPlannerContext

  val physicalSourceOp: PhysicalOperator = process(sourceOp)
  val physicalEdgeScanOp: PhysicalOperator = relEdgeScanOp
  val physicalTargetOp: PhysicalOperator = process(targetOp)

  protected val startEdgeScan: Var = ListSegment(1, list)

  /**
   * Performs the initial expand from the start node
   *
   * @param dir expand direction
   */
  protected def init(dir: ExpandDirection): PhysicalOperator = {
    val startEdgeScanOp: PhysicalOperator = physicalEdgeScanOp
      .alias(edgeScan as startEdgeScan)
      .select(startEdgeScan)

    // Execute the first expand
    val edgeJoinExpr = dir match {
      case Outbound => startEdgeScanOp.recordHeader.startNodeFor(startEdgeScan)
      case Inbound => startEdgeScanOp.recordHeader.endNodeFor(startEdgeScan)
    }

    physicalSourceOp.join(startEdgeScanOp,
      Seq(source -> edgeJoinExpr),
      InnerJoin
    ).filter(isomorphismFilter(startEdgeScan, physicalSourceOp.recordHeader.relationshipElements))
  }

  /**
   * Performs the ith expand.
   *
   * @param i              number of the iteration
   * @param iterationTable result of the i-1th iteration
   * @param directions     expansion directions
   * @param edgeVars       edges already traversed
   */
  def expand(
              i: Int,
              iterationTable: PhysicalOperator,
              directions: (ExpandDirection, ExpandDirection),
              edgeVars: Seq[Var]
            ): (PhysicalOperator, Var) = {
    val nextEdgeCT = if (i > lower) edgeScan.cypherType.nullable else edgeScan.cypherType
    val nextEdge = ListSegment(i, list)

    val aliasedEdgeScanOp = physicalEdgeScanOp.
      alias(edgeScan as nextEdge)
      .select(nextEdge)

    val iterationTableHeader = iterationTable.recordHeader
    val nextEdgeScanHeader = aliasedEdgeScanOp.recordHeader

    val joinExpr = directions match {
      case (Outbound, Outbound) => iterationTableHeader.endNodeFor(edgeVars.last) -> nextEdgeScanHeader.startNodeFor(nextEdge)
      case (Outbound, Inbound) => iterationTableHeader.endNodeFor(edgeVars.last) -> nextEdgeScanHeader.endNodeFor(nextEdge)
      case (Inbound, Outbound) => iterationTableHeader.startNodeFor(edgeVars.last) -> nextEdgeScanHeader.endNodeFor(nextEdge)
      case (Inbound, Inbound) => iterationTableHeader.startNodeFor(edgeVars.last) -> nextEdgeScanHeader.startNodeFor(nextEdge)
    }

    val expandedOp = iterationTable
      .join(aliasedEdgeScanOp, Seq(joinExpr), InnerJoin)
      .filter(isomorphismFilter(nextEdge, edgeVars.toSet))

    expandedOp -> nextEdge
  }

  /**
   * Finalize the expansions
   *   1. adds paths of length zero if needed
   *      2. fills empty columns with null values
   *      3. unions paths of different lengths
   *
   * @param paths valid paths
   */
  protected def finalize(paths: Seq[PhysicalOperator]): PhysicalOperator = {
    val targetHeader = paths.maxBy(_.recordHeader.columns.size).recordHeader

    // check whether to include paths of length 0
    val unalignedOps: Seq[PhysicalOperator] = if (lower == 0) {
      val zeroLengthExpand: PhysicalOperator = copyElement(source, target, targetHeader, physicalSourceOp)
      if (upper == 0) Seq(zeroLengthExpand) else paths :+ zeroLengthExpand
    } else paths

    // fill shorter paths with nulls
    val alignedOps = unalignedOps.map { expansion =>
      val nullExpressions = targetHeader.expressions -- expansion.recordHeader.expressions

      val expWithNullLits = expansion.addInto(nullExpressions.map(expr => NullLit -> expr).toSeq: _*)
      val exprsToRename = nullExpressions.filterNot(expr =>
        expWithNullLits.recordHeader.column(expr) == targetHeader.column(expr)
      )
      val renameTuples = exprsToRename.map(expr => expr -> targetHeader.column(expr))
      expWithNullLits.renameColumns(renameTuples.toMap)
    }

    // union expands of different lengths
    alignedOps
      .map(op => op.alignColumnNames(targetHeader))
      .reduce((agg: PhysicalOperator, next: PhysicalOperator) => TabularUnionAll(agg, next))
  }

  /**
   * Creates the isomorphism filter for the given edge list
   *
   * @param rel        new edge
   * @param candidates candidate edges
   */
  protected def isomorphismFilter(rel: Var, candidates: Set[Var]): Expr =
    Ands(candidates.map(e => Not(Equals(e, rel))).toSeq: _*)

  /**
   * Copies the content of a variable into another variable
   *
   * @param from         source variable
   * @param to           target variable
   * @param targetHeader target header
   * @param physicalOp   base operation
   */
  protected def copyElement(
                             from: Var,
                             to: Var,
                             targetHeader: RecordHeader,
                             physicalOp: PhysicalOperator
                           ): PhysicalOperator = {
    // TODO: remove when https://github.com/opencypher/morpheus/issues/513 is resolved
    val correctTarget = targetHeader.elementVars.find(_ == to).get

    val sourceChildren = targetHeader.expressionsFor(from)
    val targetChildren = targetHeader.expressionsFor(correctTarget)

    val childMapping: Set[(Expr, Expr)] = sourceChildren.map(expr => expr -> expr.withOwner(correctTarget))
    val missingMapping = (targetChildren -- childMapping.map(_._2) - correctTarget).map {
      case l: HasLabel => FalseLit -> l
      case p: Property => NullLit -> p
      case other => throw RecordHeaderException(s"$correctTarget can only own HasLabel and Property but found $other")
    }

    physicalOp.addInto((childMapping ++ missingMapping).toSeq: _*)
  }

  /**
   * Joins a given path with it's target node
   *
   * @param path the path
   * @param edge the paths last edge
   * @param dir  expand direction
   */
  protected def addTargetOps(path: PhysicalOperator, edge: Var, dir: ExpandDirection): PhysicalOperator = {
    val expr = dir match {
      case Outbound => path.recordHeader.endNodeFor(edge)
      case Inbound => path.recordHeader.startNodeFor(edge)
    }

    if (isExpandInto) {
      path.filter(Equals(target, expr))
    } else {
      path.join(physicalTargetOp, Seq(expr -> target), InnerJoin)
    }
  }
}

// TODO: use object instead
class DirectedVarLengthExpandPlanner(
                                      override val source: Var,
                                      override val list: Var,
                                      override val edgeScan: Var,
                                      override val target: Var,
                                      override val lower: Int,
                                      override val upper: Int,
                                      override val sourceOp: LogicalOperator,
                                      override val relEdgeScanOp: PhysicalOperator,
                                      override val targetOp: LogicalOperator,
                                      override val isExpandInto: Boolean
                                    )(override implicit val context: LynxPlannerContext) extends VarLengthExpandPlanner {

  override def plan: PhysicalOperator = {
    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq(init(Outbound) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val (last, edgeVars) = acc.last
        val (next, nextEdge) = expand(i, last, Outbound -> Outbound, edgeVars)
        acc :+ (next -> (edgeVars :+ nextEdge))
    }.filter(_._2.size >= lower)

    // Join target nodes on expand ops
    val withTargetOps = expandOps.map { case (op, edges) => addTargetOps(op, edges.last, Outbound) }

    finalize(withTargetOps)
  }

}

// TODO: use object instead
class UndirectedVarLengthExpandPlanner(
                                        override val source: Var,
                                        override val list: Var,
                                        override val edgeScan: Var,
                                        override val target: Var,
                                        override val lower: Int,
                                        override val upper: Int,
                                        override val sourceOp: LogicalOperator,
                                        override val relEdgeScanOp: PhysicalOperator,
                                        override val targetOp: LogicalOperator,
                                        override val isExpandInto: Boolean
                                      )(override implicit val context: LynxPlannerContext) extends VarLengthExpandPlanner {

  override def plan: PhysicalOperator = {

    val outStartOp = init(Outbound)
    val inStartOp = init(Inbound)

    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq((outStartOp -> inStartOp) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val ((last, lastRevered), edgeVars) = acc.last

        val (outOut, nextEdge) = expand(i, last, Outbound -> Outbound, edgeVars)
        val (outIn, _) = expand(i, last, Outbound -> Inbound, edgeVars)
        val (inOut, _) = expand(i, lastRevered, Inbound -> Outbound, edgeVars)
        val (inIn, _) = expand(i, lastRevered, Inbound -> Inbound, edgeVars)
        val nextOps = TabularUnionAll(outOut, inOut) -> TabularUnionAll(outIn, inIn)

        acc :+ nextOps -> (edgeVars :+ nextEdge)
    }.filter(_._2.size >= lower)


    // Join target nodes on expand ops
    val withTargetOps = expandOps.map {
      case ((out, in), edges) =>
        TabularUnionAll(
          addTargetOps(out, edges.last, Outbound),
          addTargetOps(in, edges.last, Inbound)
        )
    }

    finalize(withTargetOps)
  }

}
